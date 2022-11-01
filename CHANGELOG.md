# CHANGELOG
Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [2.x]
### Added
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Add timing data and more granular stages to SegmentReplicationState ([#4367](https://github.com/opensearch-project/OpenSearch/pull/4367))
- BWC version 2.2.2 ([#4385](https://github.com/opensearch-project/OpenSearch/pull/4385))
- Added RestLayer Changes for PIT stats ([#4217](https://github.com/opensearch-project/OpenSearch/pull/4217))
- BWC version 1.3.6 ([#4452](https://github.com/opensearch-project/OpenSearch/pull/4452))
- Bump current version to 2.4.0 on 2.x branch ([#4454](https://github.com/opensearch-project/OpenSearch/pull/4454))
- 2.3.0 release notes ([#4457](https://github.com/opensearch-project/OpenSearch/pull/4457))
- Add BWC version 2.3.1 ([#4512](https://github.com/opensearch-project/OpenSearch/pull/4512))
- Updated jackson to 2.13.4 and snakeyml to 1.32 ([#4563](https://github.com/opensearch-project/OpenSearch/pull/4563))
- Add BWC version 1.3.7 ([#4709](https://github.com/opensearch-project/OpenSearch/pull/4709))
- Bump `jettison` from 1.4.1 to 1.5.1 ([#4717](https://github.com/opensearch-project/OpenSearch/pull/4717))
- Set analyzer to regex query string search ([4219](https://github.com/opensearch-project/OpenSearch/pull/4219))
- Add dev guide for dealing with flaky tests ([4894](https://github.com/opensearch-project/OpenSearch/pull/4894))
- Renamed flaky tests ([#4935](https://github.com/opensearch-project/OpenSearch/pull/4935))
- Bump current version to 2.5.0 on 2.x branch ([#5002](https://github.com/opensearch-project/OpenSearch/pull/5002))
### Dependencies
- Update Jackson Databind to 2.13.4.2 (addressing CVE-2022-42003) ([#4781](https://github.com/opensearch-project/OpenSearch/pull/4781))
- Install and configure Log4j JUL Adapter for Lucene 9.4 ([#4754](https://github.com/opensearch-project/OpenSearch/pull/4754))
- Added feature to ignore indexes starting with dot during shard limit validation.([#4695](https://github.com/opensearch-project/OpenSearch/pull/4695))
### Changed
- Refactored BalancedAllocator.Balancer to LocalShardsBalancer ([#4818](https://github.com/opensearch-project/OpenSearch/pull/4818))
### Deprecated
### Removed
- Remove RepositoryData.MIN_VERSION support for next major release ([4729](https://github.com/opensearch-project/OpenSearch/pull/4729))
### Fixed
- PR reference to checkout code for changelog verifier ([#4296](https://github.com/opensearch-project/OpenSearch/pull/4296))
- Restore using the class ClusterInfoRequest and ClusterInfoRequestBuilder from package 'org.opensearch.action.support.master.info' for subclasses ([#4324](https://github.com/opensearch-project/OpenSearch/pull/4324))
- Fixed cancellation of segment replication events ([#4225](https://github.com/opensearch-project/OpenSearch/pull/4225))
- [Segment Replication] Add check to cancel ongoing replication with old primary on onNewCheckpoint on replica ([#4363](https://github.com/opensearch-project/OpenSearch/pull/4363))
- [Segment Replication] Bump segment infos counter before commit during replica promotion ([#4365](https://github.com/opensearch-project/OpenSearch/pull/4365))
- [Segment Replication] Update flaky testOnNewCheckpointFromNewPrimaryCancelOngoingReplication unit test ([#4414](https://github.com/opensearch-project/OpenSearch/pull/4414))
- [Segment Replication] Extend FileChunkWriter to allow cancel on transport client ([#4386](https://github.com/opensearch-project/OpenSearch/pull/4386))
- [Segment Replication] Fix NoSuchFileExceptions with segment replication when computing primary metadata snapshots ([#4366](https://github.com/opensearch-project/OpenSearch/pull/4366))
- [Segment Replication] Fix timeout issue by calculating time needed to process getSegmentFiles ([#4434](https://github.com/opensearch-project/OpenSearch/pull/4434))
- [Segment Replication] Update replicas to commit SegmentInfos instead of relying on segments_N from primary shards ([#4450](https://github.com/opensearch-project/OpenSearch/pull/4450))
- [Segment Replication] Adding check to make sure checkpoint is not processed when a shard's shard routing is primary ([#4716](https://github.com/opensearch-project/OpenSearch/pull/4716))
- Disable merge on refresh in DiskThresholdDeciderIT ([#4828](https://github.com/opensearch-project/OpenSearch/pull/4828))
- Better plural stemmer than minimal_english ([#4738](https://github.com/opensearch-project/OpenSearch/pull/4738))
- Fix AbstractStringFieldDataTestCase tests to account for TotalHits lower bound ([4867](https://github.com/opensearch-project/OpenSearch/pull/4867))
- [Segment Replication] Fix bug of replica shard's translog not purging on index flush when segment replication is enabled ([4975](https://github.com/opensearch-project/OpenSearch/pull/4975))
- Backport failures for merge conflicts on CHANGELOG.md file ([#4977](https://github.com/opensearch-project/OpenSearch/pull/4978))

### Security

[2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...2.x
