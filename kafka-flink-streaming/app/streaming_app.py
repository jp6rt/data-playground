import os
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import \
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.common.watermark_strategy import WatermarkStrategy

def my_streaming_app():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    env.add_jars(
        f"file:///{CURRENT_DIR}/lib/flink-connector-kafka-1.17.2.jar",
        f"file:///{CURRENT_DIR}/lib/kafka-clients-3.6.1.jar"
    )

    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("my-source-topic") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    
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

    ds.sink_to(sink)
    env.execute()

if __name__ == "__main__":
        my_streaming_app()
