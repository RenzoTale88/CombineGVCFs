# nf-core/combinegvcfs: Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v1.0.3 - [17/07/2026]

### Changed

- Update documentation.
- Added jemalloc to the glnexus module, allowing faster execution when available.
- FASTA indexing is now skipped if the FAI file exists.

### Fixed

- Broke anaconda environment for the FASTA indexing and interval creation.

### Removed

- `GVCF_SPLIT` process.
- Unnecessary `bcftools/concat` module.

## v1.0.2 - [15/04/2026]

### Changed

- Intervals BED files are passed to GLNexus directly, skipping intermediate steps splitting the GVCFs manually.

## v1.0.1 - [26/08/2025]

### Added

- Some utility scripts.

### Changed

- Tweaks to the shell definition in the configuration file.
- Update documentation.

## v1.0.0 - [06/02/2025]

Initial release of nf-core/combinegvcfs, created with the [nf-core](https://nf-co.re/) template.
