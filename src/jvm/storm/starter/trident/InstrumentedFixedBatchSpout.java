package storm.starter.trident;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class InstrumentedFixedBatchSpout extends FixedBatchSpout {

	static final Logger LOG = LoggerFactory
			.getLogger(InstrumentedFixedBatchSpout.class);

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {

		LOG.trace(Utils.logString("InstrumentedFixedBatchSpout", "emitBatch",
				"", "", "msgId", "" + batchId));
		super.emitBatch(batchId, collector);
	}

	@Override
	public void ack(long batchId) {
		LOG.trace(Utils.logString("InstrumentedFixedBatchSpout", "ack", "", "",
				"msgId", "" + batchId));
		super.ack(batchId);
	}

	public InstrumentedFixedBatchSpout(Fields fields, int maxBatchSize,
			List<Object>... outputs) {
		super(fields, maxBatchSize, outputs);
		// TODO Auto-generated constructor stub
	}

}
