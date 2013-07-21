package storm.starter.trident;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class InstrumentedFixedBatchSpout implements IBatchSpout {

    Fields fields;
    List<Object>[] outputs;
    int maxBatchSize;

    public InstrumentedFixedBatchSpout(Fields fields, int maxBatchSize, List<Object>... outputs) {
        this.fields = fields;
        this.outputs = outputs;
        this.maxBatchSize = maxBatchSize;
    }

    int index = 0;
    boolean cycle = false;

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        index = 0;
    }

    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }

    static final Logger LOG = LoggerFactory
            .getLogger(InstrumentedFixedBatchSpout.class);

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {

        if (LOG.isTraceEnabled())
            LOG.trace(Utils.logString("FixedBatchSpout.emitBatch", "", ""+batchId, "count", ""+maxBatchSize));

        if (index >= outputs.length && cycle) {
            index = 0;
        }
        for (int i = 0; i < maxBatchSize; index++, i++) {
            collector.emit(outputs[index % outputs.length]);
        }
    }

    @Override
    public void ack(long batchId) {
        if (LOG.isTraceEnabled())
            LOG.trace(Utils.logString("FixedBatchSpout.ack", "", ""+batchId));
    }

    @Override
    public void close() {

    }

}
