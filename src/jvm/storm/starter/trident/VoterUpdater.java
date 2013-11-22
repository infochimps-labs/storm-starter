package storm.starter.trident;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Values;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;


public class VoterUpdater extends BaseStateUpdater<ESState> {

	@Override
	public void updateState(ESState state, List<TridentTuple> tuples, TridentCollector collector) {
		List<String> ids = new ArrayList<String>();
		List<String> voters = new ArrayList<String>();
		for(TridentTuple t: tuples) {
			ids.add(t.getStringByField("key"));
			voters.add(t.getStringByField("updatedvoter"));
		}
		state.updateBulk(ids, voters);
		collector.emit(new Values(ids,voters));
	}
}
