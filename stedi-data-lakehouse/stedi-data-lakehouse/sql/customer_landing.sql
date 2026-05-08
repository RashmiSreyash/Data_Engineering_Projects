CREATE EXTERNAL TABLE customer_landing (
  customerName STRING,
  email STRING,
  phone STRING,
  birthDay STRING,
  serialNumber STRING,
  registrationDate BIGINT,
  lastUpdateDate BIGINT,
  shareWithResearchAsOfDate BIGINT,
  shareWithPublicAsOfDate BIGINT,
  shareWithFriendsAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://s3-glue-athena-udacity/customer/landing/';