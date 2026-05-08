CREATE EXTERNAL TABLE accelerometer_landing (
  user STRING,
  timestamp BIGINT,
  x DOUBLE,
  y DOUBLE,
  z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://s3-glue-athena-udacity/accelerometer/landing/';