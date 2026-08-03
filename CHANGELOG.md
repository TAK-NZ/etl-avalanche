# Changelog

All notable changes to this project will be documented in this file.

## [1.1.1]

### Changed
- CoT `time` reverted to the ETL run time. TAK Server unconditionally
  overwrites the CoT `time` attribute with its own ingestion time on
  every submitted message (confirmed in TAK Server's
  `SubmissionService.setServerTime()`), so setting it to the
  forecast's last-edited time had no effect once delivered
- `issuedLocal`/`expiresLocal` relative time suffix (e.g.
  `(3 hours ago)`) restored

## [1.1.0]

### Changed
- CoT `time` now uses the forecast's last-edited time (UTC) instead of
  the ETL run time
- CoT `stale` fallback for already-expired forecasts now steps forward
  from the true expiry time in fixed 24h increments, instead of
  `now + 24h`, so the value only changes once per day rather than on
  every ETL run
- `issuedLocal`/`expiresLocal` no longer include the relative time
  suffix (e.g. `(3 hours ago)`)

### Added
- `expired` boolean field (top-level properties and `metadata`),
  reflecting whether the forecast's true expiry time has already
  passed
- `Report has expired` line in `remarks` when `expired` is true

## [1.0.9]

### Changed
- CoT `start` now uses the forecast's issued time (UTC) instead of the
  ETL run time
- CoT `stale` now uses the forecast's expiry time (UTC) instead of a
  fixed now+24h, unless the forecast is already expired by the time
  it's fetched, in which case it falls back to now+24h

## [1.0.8]

### Fixed
- `issuedUTC` now correctly parses the upstream API's naive NZ local
  timestamps and emits proper ISO 8601 UTC strings (with `T`/`Z`), instead
  of passing the raw non-ISO string straight through
- Reordered `remarks` timestamp lines to show NZ local time before UTC

## [1.0.0] - 2024-12-19

### Added
- Initial release of ETL Avalanche
- Web scraping functionality for avalanche.net.nz
- Support for 14 avalanche regions across New Zealand
- Avalanche danger level icons (0-5)
- Structured data extraction including location, level, description, and validity dates
- TAK-compatible GeoJSON output format