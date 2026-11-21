-- Athena / AWS Glue external table over daily CSV reconciliation reports (005 FR-004, T014).
CREATE EXTERNAL TABLE IF NOT EXISTS tapi_reconciliation_daily (
    run_date          STRING,
    provider          STRING,
    expected          INT,
    terminal          INT,
    missing           INT,
    stuck             INT,
    in_flight         INT,
    redispatched      INT,
    coverage_percent  DOUBLE
)
PARTITIONED BY (partition_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'skip.header.line.count' = '1'
)
STORED AS TEXTFILE
LOCATION 's3://${RESULTS_BUCKET}/reports/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.partition_date.type' = 'date',
    'projection.partition_date.format' = 'yyyy-MM-dd',
    'projection.partition_date.range' = '2026-01-01,NOW',
    'storage.location.template' = 's3://${RESULTS_BUCKET}/reports/run_date=${partition_date}/'
);
