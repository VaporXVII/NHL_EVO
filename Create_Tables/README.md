# Overview
Contains script for creating the Delta tables that are used throughout NHL Data Pipeline. Rather than relying on Databricks to infer schemas during the initial write, tables are defined explicitly to ensure consistent field names, data types, clustering configurations, and table properties across the Medallion architecture. This approach ensures a stable foundation for the pipeline and helps prevent unintended schema changes as the project evolves.

## Purpose 
- Tables to store data from various sources are created in advance
- Script within ensures that they are created with appropriate table field, datatypes, and table configurations

## Table Configurations
- `Cluster By`: enables Databricks Liquid Clustering to improve query performance by automatically organizing underlying files based on commonly filtered fields without requiring static partitions
- `delta.autoOptimize.optimizeWrite`: Optimizes how data files are written to reduce the "small files" problem and improve downstream query performance
- `delta.enableChangeDataFeed`: Records row-level changes between table versions while leveraging Delta Lake's transaction history to support auditing, recovery, and incremental data processing.
- `delta.enableDeletionVectors`: Improves the performance of MERGE, UPDATE, DELETE, operations by avoiding unncessary file rewrites. Enabled only for large play-by-play and shift data tables where performance benefits are most significant as well as the fact that these are the only two tables that are updated multiple times a day.

## Important Note
- Some tables sizes don't exceed more than 100 rows, and in such cases, the only table configurations applied were `delta.enableChangeDataFeed = true` as the other table configurations wouldn't provide much benefit