-- EV: debezium-json (schemas.enable=false, numeric columns as JSON strings) -> upsert-kafka
SET 'execution.attached' = 'false';
SET 'pipeline.jars' = 'file:///tmp/flink-aux-smoke-aux20261012T042200Z/lib/flink-sql-connector-kafka-3.4.0-1.20.jar';
SET 'pipeline.name' = 'aux-aux20261012T042200Z-ev';
SET 'state.checkpoints.dir' = 'file:///tmp/flink-aux-smoke-aux20261012T042200Z/checkpoints/lane';
SET 'execution.checkpointing.interval' = '30s';

CREATE TABLE ev_src (
  `ID` STRING,
  `INST` STRING,
  `TAG` STRING,
  `VAL` STRING,
  `PAD` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'ocr370.live.srl.aux.aux20261012T042200Z.OLR_RAC_TEST.EV',
  'properties.bootstrap.servers' = '10.129.80.147:9092',
  'properties.group.id' = 'live-srl-aux-aux20261012T042200Z-flink-ev',
  'scan.startup.mode' = 'earliest-offset',
  'properties.isolation.level' = 'read_committed',
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
  'topic' = 'ocr370.live.srl.aux.aux20261012T042200Z.flink.sink.EV',
  'properties.bootstrap.servers' = '10.129.80.147:9092',
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
