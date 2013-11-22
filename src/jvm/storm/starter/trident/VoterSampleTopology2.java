package storm.starter.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infochimps.storm.trident.spout.FileBlobStore;
import com.infochimps.storm.trident.spout.IBlobStore;
import com.infochimps.storm.trident.spout.OpaqueTransactionalBlobSpout;
import com.infochimps.storm.trident.spout.StartPolicy;
import com.infochimps.wukong.state.WuEsState;

import storm.starter.trident.ESState.ESStateFactory;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
@SuppressWarnings({"serial","rawtypes"})
public class VoterSampleTopology2 {
    public static final Logger LOG = LoggerFactory.getLogger(VoterSampleTopology2.class);
    
    protected WukongFunctionStub wu = new WukongFunctionStub();

    
    public static class KeyGenerator extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {

        	String rawData = tuple.getString(0);
            String[] result = WukongFunctionStub.generateKey(rawData);
            System.out.println("Key ["+result[0]+"] content ["+ result[1] +"]");
            collector.emit(new Values(result[0], result[1]));
        }
    }
    
    
    public static class VoterAggregator implements CombinerAggregator<String>{

		@Override
		public String init(TridentTuple tuple) {
			System.out.println("Calling VoterAggregator.init with:"+tuple);
			if(tuple.size() == 0) return "";
			return tuple.getStringByField("content");
		}

		@Override
		public String combine(String val1, String val2) {
			System.out.println("Calling VoterAggregator.combine with val1 : "+ val1 + "val2 : "+ val2);
			return WukongFunctionStub.combine(val1, val2);
		}

		@Override
		public String zero() {
			System.out.println("Calling VoterAggregator.zero");
			return "0";
		}
    	
    }
    
    public static class MatchQuery extends BaseFunction {

    	@Override
    	public void execute(TridentTuple tuple, TridentCollector collector) {
    		String key = tuple.getStringByField("key");
    		String group = tuple.getStringByField("group");
    		
            String query = WukongFunctionStub.generateQuery(key, group);
            System.out.println("Query for key ["+ key +"] :  "+ query);
            collector.emit(new Values(query));
    	}

    }
    public static class UpdateVoter extends BaseFunction {
    	
    	@Override
    	public void execute(TridentTuple tuple, TridentCollector collector) {

    		String voter = tuple.getStringByField("voter");
    		String group = tuple.getStringByField("group");
    		String key = tuple.getStringByField("key");
    		
            String updatedVoter = WukongFunctionStub.updateVoter(key, voter, group);
            System.out.println("Updated Voter :  "+ updatedVoter);
            collector.emit(new Values(updatedVoter));
    	}
    	
    }
    
	public static class Echo extends BaseFunction {
    	@Override
    	public void execute(TridentTuple tuple, TridentCollector collector) {
    		// System.out.println("Split execute called");
    		LOG.info("echo: " + tuple.toString());
    		
    		
    	}
    }

	public static StormTopology buildTopology(LocalDRPC drpc) {
        InstrumentedFixedBatchSpout spout = new InstrumentedFixedBatchSpout(
                new Fields("line"), 5, 
                new Values("voter1;3"), 
                new Values("voter2;33"), 
                new Values("voter3;4"), 
                new Values("voter4;10"), 
                new Values("voter1;5"), 
                new Values("voter4;3")); 
        spout.setCycle(true);

//        IBlobStore bs = new FileBlobStore("/Users/sa/code/infochimps/storm-starter/data/tv");
//        OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs, StartPolicy.EARLIEST, null);
        
        List<InetSocketTransportAddress> hosts    = new ArrayList<InetSocketTransportAddress>();
        hosts.add(new InetSocketTransportAddress("localhost", 9300));

        TridentTopology topology = new TridentTopology();
        ESStateFactory fact = new ESState.ESStateFactory(hosts, "voterdb", "voter_index", "voter" );
        TridentState voters = topology.newStaticState(fact);
        
        ElasticSearchState.Options esConf = new ElasticSearchState.Options();
        esConf.clusterName = "voterdb";
        esConf.index = "voter_agg";
        esConf.type = "donations";
        

        
        topology
                .newStream("votersraw1", spout)
                .parallelismHint(1)
                .each(new Fields("line"), new KeyGenerator(), new Fields("key","content"))
                .groupBy(new Fields("key"))
                .persistentAggregate(ElasticSearchState.opaque(hosts, esConf), new Fields("content"),new VoterAggregator(), new Fields("group"))
                .newValuesStream()
                .each(new Fields("group","key"), new MatchQuery(), new Fields("query"))
                .stateQuery(voters, new Fields("query"), new VoterQuery(), new Fields("voter"))
                .each(new Fields("voter","group","key"), new UpdateVoter(), new Fields("updatedvoter"))
                .partitionPersist(fact, new Fields("key", "updatedvoter"), new VoterUpdater(), new Fields("keys","voterz"))
                .newValuesStream()
                .each(new Fields("keys", "voterz"), new Echo(), new Fields("dump"))
        ;
//        topology
//        .newStream("votersraw1", spout)
//        .parallelismHint(1)
//        .each(new Fields("line"), new KeyGenerator(), new Fields("key","content"))
//        .groupBy(new Fields("key"))
//        .aggregate(new Fields("content"),new VoterAggregator(), new Fields("group"))
//        .each(new Fields("group","key"), new MatchQuery(), new Fields("query"))
//        .stateQuery(voters, new Fields("query"), new VoterQuery(), new Fields("voter"))
//        .each(new Fields("voter","group","key"), new UpdateVoter(), new Fields("updatedvoter"))
//        .partitionPersist(fact, new Fields("key", "updatedvoter"), new VoterUpdater(), new Fields("keys","voterz"))
//        .newValuesStream()
//        .each(new Fields("keys", "voterz"), new Echo(), new Fields("dump"))
//        ;

        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setMaxSpoutPending(3);
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 5);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

        conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1500);

        System.out.println(conf);

        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        }
    }
}
