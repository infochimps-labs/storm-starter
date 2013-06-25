package storm.starter.trident;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.testing.MemoryMapState;
import backtype.storm.task.IMetricsContext;
import backtype.storm.utils.Utils;

public class InstrumentedMemoryMapState<T> extends MemoryMapState<T> {

    static final Logger LOG = LoggerFactory
            .getLogger(InstrumentedMemoryMapState.class);

    public InstrumentedMemoryMapState(String id) {
        super(id);
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys,
            List<ValueUpdater> updaters) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(Utils.logString("InstrumentedMemoryMapState",
                    "multiUpdate", "updating", "key size", "" + keys.size(),
                    "payload", updaters.toString()));
        }
        return super.multiUpdate(keys, updaters);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(Utils.logString("InstrumentedMemoryMapState", "multiPut",
                    "updating", "key size", "" + keys.size(), "payload",
                    vals.toString()));
        }

        super.multiPut(keys, vals);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {

        if (LOG.isTraceEnabled()) {
            LOG.trace(Utils.logString("InstrumentedMemoryMapState", "multiPut",
                    "updating", "key size", "" + keys.size(), "payload",
                    keys.toString()));
        }
        return super.multiGet(keys);
    }

    public static class Factory implements StateFactory {

        String _id;

        public Factory() {
            _id = UUID.randomUUID().toString();
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics,
                int partitionIndex, int numPartitions) {
            return new InstrumentedMemoryMapState(_id);
        }
    }

}
