# pipeline-server
The [IData Pipeline](https://www.idata.net) is an open-source no-code AWS data pipeline. No coding required, just configure, ingest and consume. Using a simple JSON configuration for each dataset, you can inform the Pipeline what tasks should be performed upon a dataset. 

The Pipeline can perform deduplication of data, data quality checks, transformation with Javascript, transformation into S3 parquet in your data lake using [Apache Iceberg](https://iceberg.apache.org/), or transformation into [Redshift](https://aws.amazon.com/redshift/) or [Snowflake](https://www.snowflake.com/) and much more. When data lands in its final destination, automatic notifications are fired via SNS, enabling downstream systems and users to be notified immediately.

## installation
For installation instructions, go to https://docs.idata.net/installation/