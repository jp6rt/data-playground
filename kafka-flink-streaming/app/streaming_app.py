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
from pyflink.common import Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types

class EnrichmentFn(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor('device_count', Types.INT())
        self.device_count_state = runtime_context.get_state(state_desc)

    def map(self, value):
        device_count = self.device_count_state.value()
        device_count = device_count if device_count is not None else 0
        device_count = device_count + 1
        self.device_count_state.update(device_count)
        # curr_ts = int(datetime.now().timestamp() * 1e3)
        return Row(value[0], value[1], device_count)

def my_streaming_app():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

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
            EnrichmentFn(),
            output_type=sinkd_type_info
        )

    ds.add_sink(kafka_producer)

    env.execute()

if __name__ == "__main__":
    my_streaming_app()
