Change Log
==========

All notable changes to this project will be documented in this file, which
follows the conventions of [keepachangelog.com](http://keepachangelog.com/).
This project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased

...

## [0.2.0] - 2019-05-24

### Added
- Lots of new benchmark harness code to set up a test EMR cluster in AWS.
- Enable block metering to measure store performance.

### Changed
- Upgrade to Clojure 1.10.
- Change project artifact group from `mvxcvi` to `merkle-db`.
- Bloom filters no longer pretend to be collections.
- Remove `ITable` protocol in favor of a single record type.
- Upgrade to Spark 2.4.

[Unreleased]: https://github.com/greglook/blocks/compare/0.2.0...HEAD
[0.2.0]: https://github.com/greglook/blocks/compare/0.1.0...0.2.0
