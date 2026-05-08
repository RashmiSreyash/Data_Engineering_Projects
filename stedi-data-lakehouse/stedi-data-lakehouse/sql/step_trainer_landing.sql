CREATE EXTERNAL TABLE step_trainer_landing (
  sensorReadingTime timestamp,
  serialNumber string,
  distanceFromObject DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://s3-glue-athena-udacity/step_trainer/landing/';