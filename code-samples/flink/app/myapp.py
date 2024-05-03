from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext, MapFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.state import ValueStateDescriptor
import os

APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"  # local
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

class DeviceAggregation(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor('device_count', Types.INT())
        self.device_count = runtime_context.get_state(state_desc)

    def map(self, value):
        count = self.device_count.value()
        if count is None:
            count=0
        count = count+1
        self.device_count.update(count)
        return Row(value[0], count)


def my_flink_app():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        f"file:///{CURRENT_DIR}/lib/flink-connector-kafka-1.17.2.jar",
        f"file:///{CURRENT_DIR}/lib/kafka-clients-3.6.1.jar"
    )

    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(
            type_info=Types.ROW_NAMED(["device"],[Types.STRING()])
        ).ignore_parse_errors().build()

    serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(
            type_info=Types.ROW_NAMED(["device","total_count"],[Types.STRING(), Types.INT()])
        ).build()

    kafka_consumer = FlinkKafkaConsumer(
        topics="event-consumer-topic",
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers': "localhost:9092",
            'group.id': "test_group"
        }
    )

    kafka_consumer.set_start_from_latest()

    kafka_producer = FlinkKafkaProducer(
        topic="event-aggregator-topic",
        serialization_schema=serialization_schema,
        producer_config={
            'bootstrap.servers': "localhost:9092"
        }
    )

    ds = env.add_source(kafka_consumer)
    ds = ds.key_by(lambda x: x[0]) \
        .map(
            DeviceAggregation(),
            output_type=Types.ROW_NAMED(
                ["device","total_count"],[Types.STRING(), Types.INT()]
            )
        )

    ds.add_sink(kafka_producer)
    env.execute()

if __name__ == '__main__':
    my_flink_app()

