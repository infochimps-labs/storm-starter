package storm.starter.trident;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Fields;

public class InstrumentedFixedBatchSpout extends FixedBatchSpout {

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		// TODO Auto-generated method stub
		System.out.println("InstrumentedFixedBatchSpout| emitBatch | msgId : "
				+ batchId);
		super.emitBatch(batchId, collector);
	}

	@Override
	public void ack(long batchId) {
		System.out.println("InstrumentedFixedBatchSpout| ack | msgId :"
				+ batchId);
		super.ack(batchId);
	}

	public InstrumentedFixedBatchSpout(Fields fields, int maxBatchSize,
			List<Object>... outputs) {
		super(fields, maxBatchSize, outputs);
		// TODO Auto-generated constructor stub
	}

}
