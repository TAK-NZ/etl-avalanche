# Changelog

All notable changes to this project will be documented in this file.

## [1.3.0]

### Changed

- Each region is now submitted as a single Polygon feature (`avalanche-<id>`)
  carrying the danger level `icon` and the marker position in
  `properties.center`, instead of a separate polygon and `-center` point.
  With CloudTAK running `@tak-ps/node-cot` >= 14.58.0 this becomes one
  `a-f-X-i-g-a` marker with a `<shape><polyline>`, so ATAK removes the
  region polygon when the forecast goes stale. Previously the polygon was
  converted to a `u-d-f` drawing, which never expires in ATAK.
- Regions without geometry still fall back to a Point feature, now with the
  same `avalanche-<id>` id

- Resolved all `npm audit` findings via `npm audit fix` (transitive updates
  for body-parser, fast-uri, glob, postal-mime, qs and undici, plus `eslint`
  and `typescript-eslint`). Added an `overrides` entry forcing
  `minimatch@^11.0.8` for `@typescript-eslint/typescript-estree`, which still
  pins a vulnerable 10.x; remove it once `typescript-eslint` ships a fix

### Added

- `@tak-ps/node-cot` as a devDependency, used by tests to check the
  GeoJSON to CoT conversion

### Deployment notes

- Deploy CloudTAK with `@tak-ps/node-cot` >= 14.58.0 before releasing this
  version. Older versions still convert the polygon to a non-expiring
  `u-d-f` drawing, and the icon is lost
- Existing `avalanche-<id>` `u-d-f` polygons and `avalanche-<id>-center`
  markers may remain on ATAK devices that already received them

## [1.2.0]

### Added
- `capabilities.json` manifest describing the task's compute requirements,
  permissions, and schedule invocation, embedded in the published Docker
  image as the `com.cloudtak.capabilities` OCI annotation so CloudTAK can
  read it directly from ECR
- Basic test suite (`test/basic.test.ts`) covering `Task`'s static config
  and input/output schemas, run via `tsx --test`

### Changed
- CI (`etl-deploy.yml`) now builds and pushes the Docker image with a single
  `docker buildx build` step embedding the capabilities manifest, instead of
  separate `docker build`/`tag`/`push` steps; the ECR CloudFormation-export
  lookup is unchanged
- `build-and-validate` job in `etl-deploy.yml` now runs on Node 24 (was 18),
  matching `lint.yml` and the Lambda runtime
- `task.ts` entry points (`handler` and the local-dev guard) now use
  `await Task.init(...)` instead of `new Task(...)`
- Added `engines.node: >= 24` to `package.json`
- Updated dependencies (`@tak-ps/etl` ^10.9.0 -> ^10.22.1, plus other
  `devDependencies`/transitive updates via `npm update`), resolving all
  `npm audit` findings
- Update GitHub Actions to releases that run on Node.js 24, clearing the
  Node.js 20 deprecation warnings: `actions/checkout` v7, `actions/setup-node`
  v7, `aws-actions/configure-aws-credentials` v6 and
  `docker/setup-buildx-action` v4. `aws-actions/amazon-ecr-login` v2 already
  runs on Node.js 24. Not yet run in CI on these versions
- Pin the workflow runners to `ubuntu-24.04` instead of `ubuntu-latest`, so
  the `ubuntu-latest` migration to Ubuntu 26 (starting October 19, 2026) does
  not change the build environment unannounced
- Add a .dockerignore so .git, .github, node_modules, dist, test, docs, .agents, .env*, and markdown files are kept out of the image build context

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