-- PP: debezium-json (string DECIMAL/BIGINT fields) -> upsert-kafka
SET 'execution.attached' = 'false';
SET 'pipeline.jars' = '@CONNECTOR_JAR_URI@';
SET 'pipeline.name' = '@JOB_NAME@';
SET 'state.checkpoints.dir' = '@CHECKPOINT_DIR@';
SET 'execution.checkpointing.interval' = '@CHECKPOINT_INTERVAL@';

CREATE TABLE pp_src (
  `ID` STRING,
  `CTR` STRING,
  `INST` STRING,
  `NOTE` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '@SOURCE_TOPIC_PP@',
  'properties.bootstrap.servers' = '@BOOTSTRAP_SERVERS@',
  'properties.group.id' = '@CONSUMER_GROUP@',
  'scan.startup.mode' = '@SOURCE_SCAN_MODE@',
  'properties.isolation.level' = '@SOURCE_ISOLATION_LEVEL@',
  'format' = 'debezium-json'
);

CREATE TABLE pp_sink (
  `ID` BIGINT NOT NULL,
  `CTR` BIGINT NOT NULL,
  `INST` BIGINT,
  `NOTE` STRING,
  PRIMARY KEY (`ID`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = '@SINK_TOPIC_PP@',
  'properties.bootstrap.servers' = '@BOOTSTRAP_SERVERS@',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO pp_sink
SELECT
  CAST(NULLIF(`ID`, '') AS BIGINT) AS `ID`,
  CAST(NULLIF(`CTR`, '') AS BIGINT) AS `CTR`,
  CAST(NULLIF(`INST`, '') AS BIGINT) AS `INST`,
  `NOTE`
FROM pp_src;
