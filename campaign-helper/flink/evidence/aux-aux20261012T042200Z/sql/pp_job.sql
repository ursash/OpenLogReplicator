-- PP: debezium-json (string DECIMAL/BIGINT fields) -> upsert-kafka
SET 'execution.attached' = 'false';
SET 'pipeline.jars' = 'file:///tmp/flink-aux-smoke-aux20261012T042200Z/lib/flink-sql-connector-kafka-3.4.0-1.20.jar';
SET 'pipeline.name' = 'aux-aux20261012T042200Z-pp';
SET 'state.checkpoints.dir' = 'file:///tmp/flink-aux-smoke-aux20261012T042200Z/checkpoints/lane';
SET 'execution.checkpointing.interval' = '30s';

CREATE TABLE pp_src (
  `ID` STRING,
  `CTR` STRING,
  `INST` STRING,
  `NOTE` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'ocr370.live.srl.aux.aux20261012T042200Z.OLR_RAC_TEST.PP',
  'properties.bootstrap.servers' = '10.129.80.147:9092',
  'properties.group.id' = 'live-srl-aux-aux20261012T042200Z-flink-pp',
  'scan.startup.mode' = 'earliest-offset',
  'properties.isolation.level' = 'read_committed',
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
  'topic' = 'ocr370.live.srl.aux.aux20261012T042200Z.flink.sink.PP',
  'properties.bootstrap.servers' = '10.129.80.147:9092',
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
