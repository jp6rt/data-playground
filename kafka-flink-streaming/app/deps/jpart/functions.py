from datetime import datetime, timedelta
from pyflink.datastream import RuntimeContext
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Row
from pyflink.common.typeinfo import Types

class DeviceCountFn(MapFunction):
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
