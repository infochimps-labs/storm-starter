package com.infochimps.examples;

import storm.starter.trident.InstrumentedMemoryMapState;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.infochimps.storm.trident.KafkaState;
import com.infochimps.storm.trident.spout.FileBlobStore;
import com.infochimps.storm.trident.spout.IBlobStore;
import com.infochimps.storm.trident.spout.IRecordizer;
import com.infochimps.storm.trident.spout.OpaqueTransactionalBlobSpout;
import com.infochimps.storm.trident.spout.S3BlobStore;
import com.infochimps.storm.trident.spout.StartPolicy;
import com.infochimps.storm.trident.spout.WukongRecordizer;
import com.infochimps.storm.wukong.WuFunction;

public class WukongTestTopology {
	public static class CombineMetaData extends BaseFunction {
		long line = 0;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String content = tuple.getStringByField("content");
			String metadata = tuple.getStringByField("metadata");
			Integer lineNumber = tuple.getIntegerByField("linenumber");

			line += 1;
			// System.out.print(".");
			long l = 10000;
			if (line % l == 0)
				System.out.print(line + " lines read.");

			// System.out.println(String.format("CombineMetaData called - %s\t%s\t%s\n",
			// metadata, content, lineNumber));
			// try {
			// Thread.sleep(3);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
			collector.emit(new Values(String.format("%s\t%s\t%s\n", metadata, content, lineNumber)));
		}
	}

	public static class Tracer extends BaseFunction {

		String name = "";

		public Tracer(String name) {
			super();
			this.name = name;
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {

			System.out.println(name + ":" + tuple.get(0));
			collector.emit(new Values(tuple.get(0)));
		}
	}

	public static void main(String[] args) throws Exception, InvalidTopologyException {

		System.out.println(ExampleConfig.getAll());
		String TEST_ACCESS_KEY = ExampleConfig.getString("aws.access.key"); // infochimps:s3testuser
		String TEST_SECRET_KEY = ExampleConfig.getString("aws.secret.key"); // infochimps:s3testuser
		String TEST_BUCKET_NAME = ExampleConfig.getString("aws.bucket.name");
		String TEST_ENDPOINT = ExampleConfig.getString("aws.endpoint.name");
		String prefix = ExampleConfig.getString("aws.prefix");
		
		
		int startPolicy = Integer.parseInt(ExampleConfig.getString("WukongTestTopology.startPolicy"));
		String explicitStartMarker = ExampleConfig.getString("WukongTestTopology.explicit.marker");

		int workers = Integer.parseInt(ExampleConfig.getString("storm.workers"));
		int timeout = Integer.parseInt(ExampleConfig.getString("storm.timeout"));
		int combineParallelism = Integer.parseInt(ExampleConfig.getString("storm.combine.parallelism"));
		int wukongParallelism = Integer.parseInt(ExampleConfig.getString("storm.wukong.parallelism"));

		String kafkaTopic = ExampleConfig.getString("kafka.topic");
		String zkHosts = ExampleConfig.getString("zk.hosts");// "tv-control-zk-0.tv.chimpy.us,tv-control-zk-1.tv.chimpy.us,tv-control-zk-2.tv.chimpy.us";
		
		

		IRecordizer rc = new WukongRecordizer();

		IBlobStore bs = new S3BlobStore(prefix, TEST_BUCKET_NAME, TEST_ENDPOINT, TEST_ACCESS_KEY, TEST_SECRET_KEY);

		// File Store
//		String dir = "/Users/sa/code/customers/tv/voter_files/test";
//		IBlobStore bs = new FileBlobStore(dir);
		
		
		OpaqueTransactionalBlobSpout spout =null;
		switch (startPolicy) {
		case 1:
			
			 spout = new OpaqueTransactionalBlobSpout(bs, rc, StartPolicy.EARLIEST, null);
			break;
		case 2:
			
			 spout = new OpaqueTransactionalBlobSpout(bs, rc, StartPolicy.LATEST, null);
			break;
		case 3:
			
			 spout = new OpaqueTransactionalBlobSpout(bs, rc, StartPolicy.EXPLICIT, explicitStartMarker);
			break;

		default:
			spout = new OpaqueTransactionalBlobSpout(bs, rc, StartPolicy.RESUME, null);
			break;
		}
//		OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs, rc, StartPolicy.RESUME, null);
//				"piryx/donations_meta/2013/09/03/donations-20130903-154046-0-donations-Ryan for Congress (WI-01).converted.csv.meta");

		TridentTopology topology = new TridentTopology();

		Stream source = topology.newStream("spout1", spout).parallelismHint(1).shuffle();
		Stream combine = source.each(rc.getFields(), new CombineMetaData(), new Fields("str")).parallelismHint(combineParallelism);

		if (false){//args[1].equals("wu")) {

			String dataFlowName = "identity";
			String wukongDir = "/home/arrawatia/tv/";
			String env = "production";
			System.setProperty("wukong.command", "bash -c bundle exec wu-bolt identity");
			Stream wukong = combine.each(new Fields("str"), new WuFunction(dataFlowName, wukongDir, env), new Fields("_wukong")).parallelismHint(wukongParallelism);

			wukong.partitionPersist(new KafkaState.Factory(kafkaTopic, zkHosts), new Fields("_wukong"), new KafkaState.Updater());
		} else {

			combine.partitionPersist(new KafkaState.Factory(kafkaTopic, zkHosts), new Fields("str"), new KafkaState.Updater());
		}

		topology.build();

		Config conf = new Config();
		conf.setMessageTimeoutSecs(timeout);
		// conf.setMaxSpoutPending(3);
		System.out.println("Topology created");
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordCounter", conf, topology.build());
		} else {
			conf.setNumWorkers(workers);
			StormSubmitter.submitTopology(args[0], conf, topology.build());
		}
	}
}
