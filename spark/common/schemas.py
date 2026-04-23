from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


clickstream_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("anonymous_id", StringType(), True),
        StructField("page_url", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("channel", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("browser", StringType(), True),
        StructField("country_code", StringType(), False),
        StructField("referrer", StringType(), True),
        StructField("ip_hash", StringType(), True),
        StructField("schema_version", IntegerType(), False),
    ]
)

order_schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("order_value", DoubleType(), False),
        StructField("items_count", IntegerType(), False),
        StructField("fraud_score", DoubleType(), False),
        StructField("shipping_country", StringType(), True),
        StructField("schema_version", IntegerType(), False),
    ]
)
