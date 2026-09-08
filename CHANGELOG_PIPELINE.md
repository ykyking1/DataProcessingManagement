# Pipeline Changelog

Dagster orchestration, processing, validation, reproducibility, and runtime dependency changes are recorded here.

<!-- version list -->

## pipeline-v0.1.0 (2026-09-08)


### Bug Fixes

- **pipeline**: Normalize bind-mount line endings ([`01c284f`](https://github.com/ykyking1/DataProcessingManagement/commit/01c284fb6b83d0c3704b04a69ffe3299076bba25))

- **pipeline**: Install git for lineage resolution ([`48a582e`](https://github.com/ykyking1/DataProcessingManagement/commit/48a582ee7f741b404717afb53e809ab5f29a61c5))



### Chores

- **pipeline**: Resolve component version by paths ([`43084b1`](https://github.com/ykyking1/DataProcessingManagement/commit/43084b11121523568287edd1cc128e911b4a4403))

- **release**: Configure feature branch prereleases ([`cd1e746`](https://github.com/ykyking1/DataProcessingManagement/commit/cd1e746a2862c49a047f01a5fd11d2f988f2e0fd))



### Features

- **dvc**: Add local AU-AIR repro pipeline ([`1856b9f`](https://github.com/ykyking1/DataProcessingManagement/commit/1856b9fb30773af43224ac62835c2726281bbacf))

- Add offline Ollama service and update AU-AIR data ([`842a991`](https://github.com/ykyking1/DataProcessingManagement/commit/842a991cdc9be42a7fa5c263cf2267da247abaf1))

- **pipeline**: AU-AIR akışını tek job + tek sensöre indir ([`1240666`](https://github.com/ykyking1/DataProcessingManagement/commit/124066619c9e71aec4b7a8c32ef92fd143f8a5cb))

- **dashboard**: DVC/alert artefaktları MinIO'dan, otomatik yenileme kaldırıldı ([`ef2f2e6`](https://github.com/ykyking1/DataProcessingManagement/commit/ef2f2e62b88e5c20252d2b5b965dd799151bba6b))

- **pipeline**: Gate ClickHouse data on workflow success ([`9be810b`](https://github.com/ykyking1/DataProcessingManagement/commit/9be810b3c37b3c0cf466a98424c73ce70c69365f))

- **pipeline**: Unify processing around wide AU-AIR batches ([`1ef97c8`](https://github.com/ykyking1/DataProcessingManagement/commit/1ef97c8e1f67654ef96d38ea5788ceb4a86f133d))

- **pipeline**: Integrate flight telemetry with ClickHouse dashboard ([`ed31dbe`](https://github.com/ykyking1/DataProcessingManagement/commit/ed31dbec845c28afbe7278d2b2aead557951f264))

- **pipeline**: Add versioned MinIO publication workflow ([`ddb52f0`](https://github.com/ykyking1/DataProcessingManagement/commit/ddb52f09c0fa116b8dbcb9874cdcc38ef7050aca))

- **pipeline**: Integrate Spark GE and DVC workflow ([`fdd9cce`](https://github.com/ykyking1/DataProcessingManagement/commit/fdd9ccec630b84a0ad758f5a92fadd0ecd98a6f0))

- **pipeline**: Add reproducible Spark validation stages ([`103bdf7`](https://github.com/ykyking1/DataProcessingManagement/commit/103bdf73f92c35b1e5fb64a439f33bc572d77576))

- **pipeline**: Add Spark preprocessing and GE validation ([`8d0e9a3`](https://github.com/ykyking1/DataProcessingManagement/commit/8d0e9a31f6812c1b41e2e042be5bc38f84831cdb))

- **pipeline**: Add independent semantic release stream ([`21754b0`](https://github.com/ykyking1/DataProcessingManagement/commit/21754b0ac8eea4913222c895a789ed8fe3df55f2))

- **pipeline**: Integrate Dagster outputs with DVC ([`955a29e`](https://github.com/ykyking1/DataProcessingManagement/commit/955a29e99214b641b542bb9b3bbb6d2a95105469))



### Refactoring

- **pipeline**: Consolidate active scripts ([`d8fdfaf`](https://github.com/ykyking1/DataProcessingManagement/commit/d8fdfaf9655b90ae3065c537c1c716da9ba23fcf))




## pipeline-v0.2.0 (2026-09-03)


### Features

- Add offline Ollama service and update AU-AIR data ([`842a991`](https://github.com/ykyking1/DataProcessingManagement/commit/842a991cdc9be42a7fa5c263cf2267da247abaf1))
