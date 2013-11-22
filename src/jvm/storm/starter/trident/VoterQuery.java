package storm.starter.trident;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Values;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;


public class VoterQuery extends BaseQueryFunction<ESState, String> {
    
	private static final long serialVersionUID = 1L;

	@Override
	public List<String> batchRetrieve(ESState state, List<TridentTuple> inputs) {
        List<String> matchQueries = new ArrayList<String>();
        for(TridentTuple input: inputs) {
        	matchQueries.add(input.getStringByField("query"));
        }
        return state.queryBulk(matchQueries);
    }

	@Override
    public void execute(TridentTuple tuple, String voterJSON, TridentCollector collector) {
        collector.emit(new Values(voterJSON));
    }


}