import asyncio
import json
import os
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, KafkaException
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DecimalType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

conf = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "iceberg-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(conf)

## Subscribe and poll loop
consumer.subscribe(["trades"])

# 1. Initialize Catalog
# Anchored to this file's location (project_root/consumer/.. -> project_root/warehouse),
# not the current working directory, so it works no matter where this script is invoked from.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_DIR = os.path.join(PROJECT_ROOT, "warehouse")

catalog = SqlCatalog(
    "local",
    uri=f"sqlite:///{WAREHOUSE_DIR}/catalog.db",
    warehouse="file://" + WAREHOUSE_DIR,
)

# 2. Initialize Schema
iceberg_schema = Schema(
    NestedField(
        field_id=1,
        name="type",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=2,
        name="trade_id",
        field_type=LongType(),
        required=True,
    ),
    NestedField(
        field_id=3,
        name="maker_order_id",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=4,
        name="taker_order_id",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=5,
        name="side",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=6,
        name="size",
        field_type=DecimalType(precision=18, scale=8),
        required=True,
    ),
    NestedField(
        field_id=7,
        name="price",
        field_type=DecimalType(precision=18, scale=8),
        required=True,
    ),
    NestedField(
        field_id=8,
        name="product_id",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=9,
        name="sequence",
        field_type=LongType(),
        required=True,
    ),
    NestedField(
        field_id=10,
        name="time",
        field_type=TimestampType(),
        required=True,
    ),
)

# 3. Define Partition Spec (Day transformation on timestamp)
partition_spec = PartitionSpec(
    PartitionField(source_id=10, field_id=1001, transform=DayTransform(), name="day"),
    PartitionField(
        source_id=8, field_id=1002, transform=IdentityTransform(), name="product_id"
    ),
)

# 4. Create Namespace and Table
catalog.create_namespace_if_not_exists("bronze")
table = catalog.create_table_if_not_exists(
    "bronze.trades",
    schema=iceberg_schema,
    partition_spec=partition_spec,
)


async def consume_from_redpanda():
    print("Consumer started")
    while True:
        data = consumer.consume(num_messages=50, timeout=5.0)
        batch = []

        for listItem in data:
            if listItem.error():
                if listItem.error().code() == KafkaError._PARTITION_EOF:
                    print("EOF Reached")
                else:
                    raise KafkaException(listItem.error())
                continue
            else:
                # process listItem here
                value = json.loads(listItem.value().decode())

                value["price"] = Decimal(value["price"])
                value["size"] = Decimal(value["size"])
                value["time"] = datetime.fromisoformat(
                    value["time"].replace("Z", "+00:00")
                )

                batch.append(value)

        if not batch:
            print("No messages this cycle\n\n")
            continue

        arrow_batch = pa.Table.from_pylist(batch, schema=table.schema().as_arrow())
        table.append(arrow_batch)
        consumer.commit()

        print(f"Wrote batch of {len(batch)} rows to Iceberg\n\n")


if __name__ == "__main__":
    try:
        asyncio.run(consume_from_redpanda())
    except KeyboardInterrupt:
        print("\nStream stopped")
