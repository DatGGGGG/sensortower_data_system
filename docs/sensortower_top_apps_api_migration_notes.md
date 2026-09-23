# SensorTower Top Apps API Migration Notes

Date: 2026-08-18

## Context

SensorTower announced Q1 2027 retirement for these legacy endpoints used or relevant to our account:

- `/v1/{os}/sales_report_estimates_comparison_attributes`
- `/v1/{os}/top_and_trending/active_users`
- `/v1/{os}/ad_intel/network_analysis`
- `/v1/{os}/ad_intel/network_analysis/rank`

In this repo, the confirmed production impact is the top-game snapshot stage:

- `pipeline_runtime.py` calls `/v1/{os}/sales_report_estimates_comparison_attributes`.
- Replacement is `GET /v1/facets/metrics?top_apps_mobile` with `bundle=mobile_store_top_apps`.

The current active-user fact loader calls `/v1/{os}/usage/active_users`, not the exact retired `/top_and_trending/active_users` endpoint.

The repo does not currently use `network_analysis`.

## Confirmed New Endpoint Shape

Smoke script:

```bash
python scripts/ops/smoke_test_sensortower_top_apps_api.py
```

Confirmed parameters for top-game universe migration:

- Endpoint: `/v1/facets/metrics?top_apps_mobile`
- `bundle=mobile_store_top_apps`
- `order_by=est_mobile_revenue:desc` for revenue ranking
- `breakdown=unified_app_id`
- `devices=iphone,ipad,android`
- `entities=unified_app_id:name`
- `regions=VN` and `regions=CN` work.
- For worldwide/global, omit `regions`; `regions=WW` returns HTTP 422.

Confirmed response fields:

- `unified_app_id`
- `est_mobile_downloads`
- `est_mobile_revenue`
- `est_mobile_cumulative_rpd`
- `entities.unified_app_id.<id>.name`

Active-user ranking replacement also works:

- `bundle=mobile_active_users`
- `order_by=est_mobile_mau:desc`
- Response fields: `est_mobile_dau`, `est_mobile_wau`, `est_mobile_mau`

## Pagination Finding

The official docs state that both `limit` and `offset` are supported for paging, with max `limit=2000`, and that `meta.total_count` is returned.

Observed behavior contradicts normal pagination:

```text
limit=10, offset=0  -> rows=10, meta.total_count=10
limit=10, offset=10 -> rows=0,  meta.total_count=10
limit=20, offset=10 -> rows=10, meta.total_count=20
```

This behavior was observed for both:

- `regions=VN`
- worldwide/global with `regions` omitted

It also persisted when encoding the flag exactly as `/metrics?top_apps_mobile` instead of passing `top_apps_mobile=`.

Interpretation:

- `offset` appears to be applied after first truncating to `limit`.
- `meta.total_count` appears to echo the requested/returned top-N window, not the true total available universe.

## Migration Risk

The current legacy snapshot stage scans up to 10,000 candidates per market/year.

Because the new endpoint has documented `limit` max of 2,000 and pagination appears not to work normally, a production adapter cannot yet safely reproduce the old 10,000-candidate scan.

Adapter v1 could fetch top 2,000 per market/year, but this may reduce coverage versus the current pipeline.

## Question For SensorTower Support

Please ask SensorTower:

```text
We are migrating from /v1/{os}/sales_report_estimates_comparison_attributes to
/v1/facets/metrics?top_apps_mobile.

The migration guide says limit and offset are supported, and limit max is 2,000.
However, in our tests:

- limit=10&offset=0 returns 10 rows
- limit=10&offset=10 returns 0 rows
- limit=20&offset=10 returns 10 rows

This suggests offset is applied after truncating to limit, instead of normal
pagination behavior. meta.total_count also appears to match the requested
limit rather than the true total result count.

How should we paginate beyond the first 2,000 rows for:

- bundle=mobile_store_top_apps
- breakdown=unified_app_id
- devices=iphone,ipad,android
- order_by=est_mobile_revenue:desc
- regions=VN / CN, and global with no regions filter?

Our current production workflow scans up to 10,000 candidates per market/year,
so we need equivalent pagination before fully migrating.
```

## 2026-09-23 Resolution And Implementation

SensorTower support reported that the pagination defect was fixed on 2026-09-22.
Live verification on 2026-09-23 confirmed normal paging:

```text
VN: limit=10, offset=10 -> rows=10, meta.total_count=35,565
WW: limit=10, offset=10 -> rows=10, meta.total_count=174,297
```

The worldwide request used the documented route marker and omitted `regions`.

`pipeline_runtime.py` now uses:

- `GET /v1/facets/metrics?top_apps_mobile`
- `bundle=mobile_store_top_apps`
- `order_by=est_mobile_revenue:desc`
- `breakdown=unified_app_id`
- `devices=iphone,ipad,android`
- `entities=unified_app_id:name`
- explicit annual `start_date` and `end_date`
- `regions` for VN/CN and no `regions` parameter for WW

The adapter maps `unified_app_id` and `est_mobile_revenue` back into the existing
snapshot output contract. Revenue remains converted from cents to currency units.
The old 10,000-offset cap was removed because repaired pagination can now scan up
to the configured `TOP_GAME_SNAPSHOT_MAX_CANDIDATE_COUNT`.

Legacy settings that have no Top Apps equivalent are not sent. The adapter fails
fast if configured for growth/delta comparison, a non-annual time range, or a
non-unified snapshot instead of silently changing semantics.

## 2026-08-19 Pause Decision

Status at the time: paused pending SensorTower confirmation.

Decision:

- Keep the existing production source/base pipeline on the legacy top-game endpoint for now.
- Do not enable the Top Apps Facets adapter in weekly or quarterly cron yet.
- Do not spend more SensorTower requests on large Top Apps Facets pagination tests until SensorTower answers the support question.
- Continue unrelated stabilization work using existing core/base data only.

Resume migration only when SensorTower confirms:

- The correct pagination pattern for `/v1/facets/metrics?top_apps_mobile`.
- Whether results beyond the first 2,000 ranked apps are accessible.
- Whether `meta.total_count` is expected to represent the true full result count or only the requested top-N window.
- Whether omitting `regions` is the official worldwide/global ranking behavior.

These conditions were satisfied by the support update and the 2026-09-23 live
checks above. This pause is retained as historical context and is no longer active.

Safe interim work:

- Keep/update the smoke script and notes.
- Add adapter scaffolding behind a disabled feature flag if needed.
- Avoid changing production cron behavior for this migration until old/new parity can be tested.
