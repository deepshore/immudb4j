# Changelog

## 1.5.1

### Bug Fixes

- **Fix session leak**: Cancel heartbeater before closing session and channel. `closeSession()` is now idempotent and swallows RPC failures. `shutdown()` cancels the heartbeater independently so an interrupted or failed `CloseSession` RPC can no longer leave an orphaned Timer thread that keeps the server-side session alive indefinitely.
- **Fix integration tests**: Tests were failing because TestNG interleaves methods by name across classes, causing shared static `immuClient` mutations in one test to break others. Each test now manages its own isolated client or cleans up properly.
- **Fix sourcesJar dependency**: `sourcesJar`/`javadocJar` now explicitly depend on `generateProto` to avoid Gradle implicit dependency warnings. Signing is only required for non-snapshot publish tasks.

### Features

- **Configurable heartbeat**: Allow disabling the internal heartbeat timer and expose `keepAlive()` for external scheduling.
- **Unit tests for session lifecycle**: Added tests covering session lifecycle edge cases.

### Build & CI

- **Gradle update**: Upgraded Gradle wrapper.
- **Workflow and test version updates**: Updated CI workflow and test dependency versions.
- **Updated immudb test image**: Bumped immudb Docker image used in integration tests.
- **Replace coveralls plugin**: Replaced unmaintained `com.github.kt3k.coveralls` (incompatible with Gradle 7.6.4) with `com.github.nbaztec.coveralls-jacoco 1.2.20`.
- **Conditional coveralls**: Skip coveralls step when `COVERALLS_REPO_TOKEN` is not set.
- **Publish workflow**: Added publish workflow for Sonatype Central on the deepshore fork. Snapshots on push, releases on GitHub release event.
- **Snapshot from feature branch**: Enabled snapshot publishing from feature branches.
- **New group and version**: Changed Maven group to `io.github.deepshore`, version to `1.5.1`.
- **Suppress javadoc warnings**: Suppressed warnings from generated protobuf code.
- **Java 9+ compatibility**: Replaced `_` with `ignored` in catch blocks for compatibility.
- **Artifact links on release**: Publish workflow now appends Maven Central artifact links to GitHub releases.
- **Manual workflow trigger**: Added `workflow_dispatch` and switched snapshot trigger from push to pull request.