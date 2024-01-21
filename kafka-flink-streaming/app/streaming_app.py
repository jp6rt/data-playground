import os
import json
from datetime import datetime, timedelta
from pyflink.datastream import \
    StreamExecutionEnvironment, RuntimeExecutionMode, RuntimeContext
from pyflink.datastream.connectors.kafka import \
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema, DeliveryGuarantee, FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Row, Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from jpart.functions import DeviceCountFn

def my_streaming_app():
    config = Configuration()
    config.set_string("python.execution-mode", "thread")
    config.set_integer("python.fn-execution.bundle.time", 10)
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    env.add_jars(
        f"file:///{CURRENT_DIR}/lib/flink-connector-kafka-1.17.2.jar",
        f"file:///{CURRENT_DIR}/lib/kafka-clients-3.6.1.jar"
    )

    sourced_type_info = Types.ROW_NAMED(
        ["ts", "device"],
        [Types.LONG(), Types.STRING()]
    )

    sinkd_type_info = Types.ROW_NAMED(
        ["ts", "device", "device_count"],
        [Types.LONG(), Types.STRING(), Types.INT()]
    )

    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(
            type_info=sourced_type_info
        ).ignore_parse_errors().build()

    serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(
            type_info=sinkd_type_info
        ).build()

    kafka_consumer = FlinkKafkaConsumer(
        topics="my-source-topic",
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers': "localhost:9092",
            'group.id': "test_group"
        }
    )

    kafka_consumer.set_start_from_latest()

    kafka_producer = FlinkKafkaProducer(
        topic="my-sink-topic",
        serialization_schema=serialization_schema,
        producer_config={
            'bootstrap.servers': "localhost:9092"
        }
    )

    ds = env.add_source(kafka_consumer)
    # key by second index which is the deviceid
    ds = ds.key_by(lambda row: row[1])  \
        .map(
            DeviceCountFn(),
            output_type=sinkd_type_info
        )

    ds.add_sink(kafka_producer)

    env.execute()

if __name__ == "__main__":
    my_streaming_app()
