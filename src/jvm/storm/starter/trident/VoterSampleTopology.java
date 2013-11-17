package storm.starter.trident;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infochimps.storm.trident.spout.FileBlobStore;
import com.infochimps.storm.trident.spout.IBlobStore;
import com.infochimps.storm.trident.spout.OpaqueTransactionalBlobSpout;
import com.infochimps.storm.trident.spout.StartPolicy;
import com.infochimps.wukong.state.WuEsState;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
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

public class VoterSampleTopology {
    public static final Logger LOG = LoggerFactory.getLogger(VoterSampleTopology.class);
    
    protected WukongFunctionStub wu = new WukongFunctionStub();

    
    public static class KeyGenerator extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            // System.out.println("Split execute called");
//            LOG.info("splitting: " + tuple.toString());

            String sentence = tuple.getString(0);
            String[] word = WukongFunctionStub.generateKey(sentence);
            System.out.println("Key ["+word[0]+"] content ["+ word[1] +"]");
            collector.emit(new Values(word[0],word[1]));
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
    
    public static class Echo extends BaseFunction {
    	@Override
    	public void execute(TridentTuple tuple, TridentCollector collector) {
    		// System.out.println("Split execute called");
    		LOG.info("echo: " + tuple.toString());
    		
    		
    	}
    }

    public static StormTopology buildTopology(LocalDRPC drpc) {
//        InstrumentedFixedBatchSpout spout = new InstrumentedFixedBatchSpout(
//                new Fields("emails"), 5, 
//                new Values("voter1;the cow jumped over the moon"), 
//                new Values("voter1;the man went to the store and bought some candy"),
//                new Values("voter2;four score and seven years ago"), 
//                new Values("voter3;how many apples can you eat"), 
//                new Values("voter3;to be or not to be the person"));
//        spout.setCycle(true);

        IBlobStore bs = new FileBlobStore("/Users/sa/code/infochimps/storm-starter/data/tv");
        OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs, StartPolicy.EARLIEST, null);
        
        List<InetSocketTransportAddress> hosts    = new ArrayList<InetSocketTransportAddress>();
          hosts.add(new InetSocketTransportAddress("localhost", 9300));

        ElasticSearchState.Options esConf = new ElasticSearchState.Options();
        esConf.clusterName = "voterdb";
        esConf.indexSuffix = "voter";
        
        TridentTopology topology = new TridentTopology();
        topology
                .newStream("votersraw1", spout)
                .parallelismHint(1)
                .each(new Fields("line"), new KeyGenerator(), new Fields("key","content"))
                .groupBy(new Fields("key"))
//                .persistentAggregate(new VoterState.Factory(),new Fields("content"),new VoterAggregator(), new Fields("count"))
                .persistentAggregate(ElasticSearchState.opaque(hosts, esConf),new Fields("content"),new VoterAggregator(), new Fields("count"))
                .newValuesStream()
                .each(new Fields("count","key"), new Echo(), new Fields("abc"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setMaxSpoutPending(3);
        conf.setDebug(false);
        conf.put(conf.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 5);
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
