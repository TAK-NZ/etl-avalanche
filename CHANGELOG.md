# Changelog

All notable changes to this project will be documented in this file.

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