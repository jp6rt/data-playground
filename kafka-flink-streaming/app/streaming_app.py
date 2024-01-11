import os
import json
from datetime import datetime, timedelta
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import \
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema, DeliveryGuarantee, FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common import Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types

class AddTagFn(MapFunction):
    def map(self, value):
        # value['flink_tag'] = (datetime.now() + timedelta()).isoformat()
        # value['flink_tag'] = "tag"
        return Row(value[0], value[1], "tag")

def my_streaming_app():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    env.add_jars(
        f"file:///{CURRENT_DIR}/lib/flink-connector-kafka-1.17.2.jar",
        f"file:///{CURRENT_DIR}/lib/kafka-clients-3.6.1.jar"
    )

    """
    # This code is based from the Flink kafka documentation but it doesn't work on 1.17
    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("my-source-topic") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


    sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder() \
                .set_topic("my-sink-topic") \
                .set_value_serialization_schema(SimpleStringSchema()) \
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    ds.map(lambda row: json.loads(row)) \
        .map(lambda row: {"foo": "bar"}) \
        .map(lambda row: json.dumps(row), output_type=Types.STRING())

    ds.sink_to(sink)
    """

    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(
            type_info=Types.ROW_NAMED(
                ["ts", "device"],
                [Types.STRING(), Types.STRING()]
            )
        ).ignore_parse_errors().build()

    serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(
            type_info=Types.ROW_NAMED(
                ["ts", "device", "flink_tag"],
                [Types.STRING(), Types.STRING(), Types.STRING()]
            )
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
            AddTagFn(),
           output_type=Types.ROW_NAMED(
               ["ts", "device", "flink_tag"],
               [Types.STRING(), Types.STRING(), Types.STRING()]
           )
        )

    ds.add_sink(kafka_producer)

    env.execute()

if __name__ == "__main__":
    my_streaming_app()
