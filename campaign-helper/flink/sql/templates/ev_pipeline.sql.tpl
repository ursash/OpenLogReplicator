-- EV: debezium-json (schemas.enable=false, numeric columns as JSON strings) -> upsert-kafka
SET 'execution.attached' = 'false';
SET 'pipeline.jars' = '@CONNECTOR_JAR_URI@';
SET 'pipeline.name' = '@JOB_NAME@';
SET 'state.checkpoints.dir' = '@CHECKPOINT_DIR@';
SET 'execution.checkpointing.interval' = '@CHECKPOINT_INTERVAL@';

CREATE TABLE ev_src (
  `ID` STRING,
  `INST` STRING,
  `TAG` STRING,
  `VAL` STRING,
  `PAD` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '@SOURCE_TOPIC_EV@',
  'properties.bootstrap.servers' = '@BOOTSTRAP_SERVERS@',
  'properties.group.id' = '@CONSUMER_GROUP@',
  'scan.startup.mode' = '@SOURCE_SCAN_MODE@',
  'properties.isolation.level' = '@SOURCE_ISOLATION_LEVEL@',
  'format' = 'debezium-json'
);

CREATE TABLE ev_sink (
  `ID` BIGINT NOT NULL,
  `INST` BIGINT,
  `TAG` STRING,
  `VAL` BIGINT,
  `PAD` STRING,
  PRIMARY KEY (`ID`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = '@SINK_TOPIC_EV@',
  'properties.bootstrap.servers' = '@BOOTSTRAP_SERVERS@',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO ev_sink
SELECT
  CAST(NULLIF(`ID`, '') AS BIGINT) AS `ID`,
  CAST(NULLIF(`INST`, '') AS BIGINT) AS `INST`,
  `TAG`,
  CAST(NULLIF(`VAL`, '') AS BIGINT) AS `VAL`,
  `PAD`
FROM ev_src;
