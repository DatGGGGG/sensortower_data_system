from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class OpenApiModel(BaseModel):
    """Keep documented response models forward-compatible with warehouse columns."""

    model_config = ConfigDict(extra="allow")


class ErrorResponse(OpenApiModel):
    detail: str | dict[str, Any] = Field(description="Human-readable error detail or structured error payload.")


class GameSearchItem(OpenApiModel):
    unified_app_id: str
    game_name: str | None = None
    canonical_app_id: str | None = None
    game_class: str | None = None
    game_genre: str | None = None
    game_subgenre: str | None = None


class GameSearchResponse(OpenApiModel):
    items: list[GameSearchItem]
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)


class GameListItem(OpenApiModel):
    unified_app_id: str
    canonical_app_id: str | None = None
    name: str | None = None
    game_class: str | None = None
    game_genre: str | None = None
    game_subgenre: str | None = None
    game_release_date_ww: date | None = None
    linked_app_count: int = Field(ge=0)


class GameListResponse(OpenApiModel):
    items: list[GameListItem]
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)


class GameDetailResponse(OpenApiModel):
    unified_app_id: str
    canonical_app_id: str | None = None
    name: str | None = None
    game_class: str | None = None
    game_genre: str | None = None
    game_subgenre: str | None = None
    linked_app_count: int = Field(ge=0)


class AppListItem(OpenApiModel):
    app_id: str
    name: str | None = None
    publisher_name: str | None = None
    os: str | None = None
    active: bool | None = None
    price: float | None = None
    rating: float | None = None
    global_rating_count: int | None = Field(default=None, ge=0)
    unified_app_id: str | None = None
    game_name: str | None = None
    game_genre: str | None = None
    game_class: str | None = None


class AppListResponse(OpenApiModel):
    items: list[AppListItem]
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)


class AppDetailResponse(OpenApiModel):
    app_id: str
    name: str | None = None
    publisher_name: str | None = None
    os: str | None = None
    unified_app_id: str | None = None
    game_name: str | None = None
    game_class: str | None = None
    game_genre: str | None = None
    game_subgenre: str | None = None


class AppPerformanceItem(OpenApiModel):
    app_id: str
    country_android: str | None = None
    country_ios: str | None = None
    date: date
    downloads_android: int | None = None
    downloads_iphone: int | None = None
    downloads_ipad: int | None = None
    revenue_android: int | None = None
    revenue_iphone: int | None = None
    revenue_ipad: int | None = None


class AppPerformanceResponse(OpenApiModel):
    app: AppDetailResponse
    items: list[AppPerformanceItem]
    count: int = Field(ge=0)
    limit: int = Field(ge=1)


class CatalogColumn(OpenApiModel):
    name: str
    data_type: str
    nullable: bool
    semantic_type: str
    unit: str
    additivity: str
    default_aggregation: str
    allowed_aggregations: list[str]
    null_policy: str


class CatalogObject(OpenApiModel):
    name: str
    object_type: str
    grain: str
    description: str
    columns: list[CatalogColumn]
    examples: list[dict[str, str]]


class CatalogResponse(OpenApiModel):
    objects: list[CatalogObject]
