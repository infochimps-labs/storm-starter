package com.infochimps.storm.testrig.topology;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.testing.MemoryMapState;
import backtype.storm.task.IMetricsContext;

public class InstrumentedMemoryMapState<T> extends MemoryMapState<T> {

    static Logger LOG = Logger.getLogger(InstrumentedMemoryMapState.class);

    public InstrumentedMemoryMapState(String id) {
        super(id);
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys,
            List<ValueUpdater> updaters) {
        LOG.info(String.format("..." + "  \tupdating\t%s\t%s", keys.size(),
                updaters));
        return super.multiUpdate(keys, updaters);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {

        LOG.info(String.format("..." + "  \twriting\t%s\t%s", keys.size(), vals));
        System.out.println("get");
        super.multiPut(keys, vals);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        LOG.info(String.format("..." + "  \tgetting\t%s\t", keys.size()));
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
