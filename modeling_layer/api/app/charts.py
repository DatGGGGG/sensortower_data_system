from __future__ import annotations

from collections import OrderedDict, defaultdict
import binascii
from datetime import UTC, datetime, timedelta
import base64
import hashlib
import hmac
import json
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import plotly.graph_objects as go
from pydantic import BaseModel, Field, model_validator

from .config import Settings


class ChartValidationError(ValueError):
    pass


class ChartArtifactAccessError(ValueError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class ChartSpec(BaseModel):
    chart_type: Literal["line", "bar", "grouped_bar", "stacked_bar", "scatter", "histogram", "donut", "table"]
    title: str = Field(min_length=1, max_length=200)
    x: str | None = Field(default=None, min_length=1, max_length=128)
    y: str | None = Field(default=None, min_length=1, max_length=128)
    series: str | None = Field(default=None, min_length=1, max_length=128)
    sort: Literal["x_asc", "x_desc", "y_asc", "y_desc"] | None = None
    x_type: Literal["category", "time", "numeric"] | None = None
    y_format: Literal["integer", "number", "currency", "percent"] | None = None
    legend: bool = True

    @model_validator(mode="after")
    def validate_axes(self) -> "ChartSpec":
        if self.chart_type == "table":
            return self
        if self.chart_type == "histogram":
            if not self.x:
                raise ValueError("x is required for histogram charts.")
            return self
        if not self.x or not self.y:
            raise ValueError("x and y are required for this chart type.")
        return self


class RenderChartRequest(BaseModel):
    data: list[dict[str, Any]] = Field(min_length=1)
    spec: ChartSpec
    source: dict[str, Any] | None = None


def render_chart_artifact(
    payload: RenderChartRequest,
    settings: Settings,
    base_url: str,
) -> dict[str, Any]:
    cleanup_expired_chart_artifacts(settings)
    validate_chart_request(payload, settings.chart_max_rows)

    rows = sort_rows(payload.data, payload.spec)
    figure = build_figure(rows, payload.spec)
    artifact_id = str(uuid4())
    created_at = datetime.now(UTC)
    expires_at = created_at + timedelta(hours=settings.chart_ttl_hours)
    expires_at_epoch = int(expires_at.timestamp())

    artifact_dir = ensure_artifact_dir(settings)
    html_path = artifact_dir / f"{artifact_id}.html"
    metadata_path = artifact_dir / f"{artifact_id}.json"

    html_path.write_text(
        figure.to_html(
            include_plotlyjs="cdn",
            full_html=True,
            config={"responsive": True, "displaylogo": False},
        ),
        encoding="utf-8",
    )

    metadata = {
        "artifact_id": artifact_id,
        "chart_type": payload.spec.chart_type,
        "title": payload.spec.title,
        "row_count": len(rows),
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "expires_at_epoch": expires_at_epoch,
        "html_file": html_path.name,
        "source": payload.source or {},
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    token = generate_chart_token(artifact_id, expires_at_epoch, settings.chart_signing_secret)
    public_base = (settings.public_base_url or base_url).rstrip("/")
    chart_url = f"{public_base}/charts/artifacts/{artifact_id}?token={token}"

    return {
        "artifact_id": artifact_id,
        "chart_url": chart_url,
        "expires_at": expires_at.isoformat(),
        "chart_type": payload.spec.chart_type,
        "row_count": len(rows),
    }


def load_chart_artifact(artifact_id: str, token: str, settings: Settings) -> Path:
    cleanup_expired_chart_artifacts(settings)

    artifact_dir = ensure_artifact_dir(settings)
    metadata_path = artifact_dir / f"{artifact_id}.json"
    if not metadata_path.exists():
        raise ChartArtifactAccessError(404, "Chart artifact not found.")

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    expires_at_epoch = int(metadata.get("expires_at_epoch", 0))

    validate_chart_token(artifact_id, token, expires_at_epoch, settings.chart_signing_secret)

    if int(datetime.now(UTC).timestamp()) > expires_at_epoch:
        remove_artifact_files(artifact_dir, artifact_id, metadata.get("html_file"))
        raise ChartArtifactAccessError(410, "Chart artifact has expired.")

    html_file = metadata.get("html_file")
    html_path = artifact_dir / html_file
    if not html_file or not html_path.exists():
        raise ChartArtifactAccessError(404, "Chart artifact file is missing.")

    return html_path


def cleanup_expired_chart_artifacts(settings: Settings) -> None:
    artifact_dir = ensure_artifact_dir(settings)
    now_epoch = int(datetime.now(UTC).timestamp())

    for metadata_path in artifact_dir.glob("*.json"):
        artifact_id = metadata_path.stem
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            remove_artifact_files(artifact_dir, artifact_id, None)
            continue

        expires_at_epoch = int(metadata.get("expires_at_epoch", 0))
        html_file = metadata.get("html_file")
        if expires_at_epoch <= now_epoch:
            remove_artifact_files(artifact_dir, artifact_id, html_file)


def validate_chart_request(payload: RenderChartRequest, max_rows: int) -> None:
    if not payload.data:
        raise ChartValidationError("Chart data is required.")
    if len(payload.data) > max_rows:
        raise ChartValidationError(f"Chart data may contain at most {max_rows} rows.")

    available_fields = list(collect_fields(payload.data))
    required_fields = [field for field in (payload.spec.x, payload.spec.y, payload.spec.series) if field]
    missing_fields = [field for field in required_fields if field not in available_fields]
    if missing_fields:
        joined = ", ".join(sorted(set(missing_fields)))
        raise ChartValidationError(f"Chart spec references missing field(s): {joined}")


def build_figure(rows: list[dict[str, Any]], spec: ChartSpec) -> go.Figure:
    if spec.chart_type == "table":
        return build_table_figure(rows, spec)
    if spec.chart_type == "donut":
        return build_donut_figure(rows, spec)
    if spec.chart_type == "histogram":
        return build_histogram_figure(rows, spec)

    traces: list[Any] = []
    grouped_rows = group_rows(rows, spec.series)

    for series_name, series_rows in grouped_rows.items():
        x_values = [row.get(spec.x or "") for row in series_rows]
        y_values = [row.get(spec.y or "") for row in series_rows]
        trace_name = series_name if spec.series else spec.title

        if spec.chart_type == "line":
            traces.append(go.Scatter(x=x_values, y=y_values, mode="lines+markers", name=trace_name))
        elif spec.chart_type == "scatter":
            traces.append(go.Scatter(x=x_values, y=y_values, mode="markers", name=trace_name))
        else:
            traces.append(go.Bar(x=x_values, y=y_values, name=trace_name))

    figure = go.Figure(data=traces)
    layout: dict[str, Any] = {
        "title": spec.title,
        "showlegend": bool(spec.series and spec.legend),
        "template": "plotly_white",
        "margin": {"l": 48, "r": 24, "t": 72, "b": 48},
    }
    if spec.chart_type == "stacked_bar":
        layout["barmode"] = "stack"
    elif spec.chart_type == "grouped_bar":
        layout["barmode"] = "group"

    if spec.x_type == "time":
        layout["xaxis"] = {"type": "date"}
    elif spec.x_type == "numeric":
        layout["xaxis"] = {"type": "linear"}

    yaxis = format_y_axis(spec.y_format)
    if yaxis:
        layout["yaxis"] = yaxis

    figure.update_layout(**layout)
    return figure


def build_table_figure(rows: list[dict[str, Any]], spec: ChartSpec) -> go.Figure:
    columns = list(collect_fields(rows))
    column_values = [[row.get(column) for row in rows] for column in columns]
    figure = go.Figure(
        data=[
            go.Table(
                header={"values": columns, "fill_color": "#1f2937", "font": {"color": "white"}},
                cells={"values": column_values, "align": "left"},
            )
        ]
    )
    figure.update_layout(
        title=spec.title,
        template="plotly_white",
        margin={"l": 24, "r": 24, "t": 72, "b": 24},
    )
    return figure


def build_donut_figure(rows: list[dict[str, Any]], spec: ChartSpec) -> go.Figure:
    labels = [row.get(spec.x or "") for row in rows]
    values = [row.get(spec.y or "") for row in rows]
    figure = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.5,
                sort=False,
                textinfo="label+percent",
            )
        ]
    )
    figure.update_layout(
        title=spec.title,
        showlegend=spec.legend,
        template="plotly_white",
        margin={"l": 24, "r": 24, "t": 72, "b": 24},
    )
    return figure


def build_histogram_figure(rows: list[dict[str, Any]], spec: ChartSpec) -> go.Figure:
    traces: list[Any] = []
    grouped_rows = group_rows(rows, spec.series)

    for series_name, series_rows in grouped_rows.items():
        x_values = [row.get(spec.x or "") for row in series_rows]
        trace_name = series_name if spec.series else spec.title
        traces.append(
            go.Histogram(
                x=x_values,
                name=trace_name,
                opacity=0.75 if spec.series else 1.0,
            )
        )

    figure = go.Figure(data=traces)
    layout: dict[str, Any] = {
        "title": spec.title,
        "showlegend": bool(spec.series and spec.legend),
        "template": "plotly_white",
        "margin": {"l": 48, "r": 24, "t": 72, "b": 48},
        "barmode": "overlay" if spec.series else "relative",
    }
    if spec.x_type == "time":
        layout["xaxis"] = {"type": "date"}
    elif spec.x_type == "numeric":
        layout["xaxis"] = {"type": "linear"}

    figure.update_layout(**layout)
    return figure


def sort_rows(rows: list[dict[str, Any]], spec: ChartSpec) -> list[dict[str, Any]]:
    if not spec.sort:
        return list(rows)

    field = spec.x if spec.sort.startswith("x_") else spec.y
    if not field:
        return list(rows)

    reverse = spec.sort.endswith("_desc")
    return sorted(rows, key=lambda row: make_sortable(row.get(field)), reverse=reverse)


def group_rows(rows: list[dict[str, Any]], series_field: str | None) -> OrderedDict[str | None, list[dict[str, Any]]]:
    if not series_field:
        return OrderedDict({None: rows})

    grouped: defaultdict[str | None, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[normalize_series_name(row.get(series_field))].append(row)

    ordered = OrderedDict()
    for name in grouped:
        ordered[name] = grouped[name]
    return ordered


def collect_fields(rows: list[dict[str, Any]]) -> OrderedDict[str, None]:
    ordered_fields: OrderedDict[str, None] = OrderedDict()
    for row in rows:
        for key in row:
            ordered_fields.setdefault(key, None)
    return ordered_fields


def format_y_axis(y_format: str | None) -> dict[str, Any]:
    if y_format == "integer":
        return {"tickformat": ",.0f"}
    if y_format == "number":
        return {"tickformat": ",.2f"}
    if y_format == "currency":
        return {"tickprefix": "$", "tickformat": ",.2f"}
    if y_format == "percent":
        return {"tickformat": ".2%"}
    return {}


def make_sortable(value: Any) -> tuple[int, str]:
    if value is None:
        return (1, "")
    if isinstance(value, (int, float)):
        return (0, f"{value:020.6f}")
    return (0, str(value))


def normalize_series_name(value: Any) -> str:
    if value is None or value == "":
        return "Series"
    return str(value)


def ensure_artifact_dir(settings: Settings) -> Path:
    artifact_dir = Path(settings.chart_artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def generate_chart_token(artifact_id: str, expires_at_epoch: int, secret: str) -> str:
    payload = f"{artifact_id}:{expires_at_epoch}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
    return f"{expires_at_epoch}.{urlsafe_b64encode(signature)}"


def validate_chart_token(
    artifact_id: str,
    token: str,
    expires_at_epoch: int,
    secret: str,
) -> None:
    try:
        token_expiry, encoded_signature = token.split(".", 1)
        token_expiry_int = int(token_expiry)
    except ValueError as exc:
        raise ChartArtifactAccessError(403, "Invalid chart token.") from exc

    if token_expiry_int != expires_at_epoch:
        raise ChartArtifactAccessError(403, "Chart token does not match artifact expiry.")
    if int(datetime.now(UTC).timestamp()) > token_expiry_int:
        raise ChartArtifactAccessError(410, "Chart token has expired.")

    payload = f"{artifact_id}:{token_expiry_int}".encode("utf-8")
    expected_signature = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
    actual_signature = urlsafe_b64decode(encoded_signature)
    if not hmac.compare_digest(expected_signature, actual_signature):
        raise ChartArtifactAccessError(403, "Invalid chart token signature.")


def urlsafe_b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("utf-8").rstrip("=")


def urlsafe_b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(f"{value}{padding}")
    except (ValueError, binascii.Error) as exc:
        raise ChartArtifactAccessError(403, "Invalid chart token signature.") from exc


def remove_artifact_files(artifact_dir: Path, artifact_id: str, html_file: str | None) -> None:
    paths = [artifact_dir / f"{artifact_id}.json"]
    if html_file:
        paths.append(artifact_dir / html_file)
    else:
        paths.append(artifact_dir / f"{artifact_id}.html")

    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            continue
