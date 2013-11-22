package storm.starter.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

@SuppressWarnings({"serial","rawtypes"})
public class ESState implements State {

	private final Logger LOG = Logger.getLogger(ESState.class);
	private final Client _client;
	private String _type;
	private String _index;

	public ESState(List<InetSocketTransportAddress> servers, String clusterName, String index, String type) {
		_client = makeClient(servers, clusterName);
		_index = index;
		_type = type;
	}

	private Client makeClient(List<InetSocketTransportAddress> endpoints, String clusterName) {
		LOG.info("Creating client ....");
		
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
		TransportClient client = new TransportClient(settings);
		for (InetSocketTransportAddress address : endpoints) {
			client.addTransportAddress(address);
		}
		
		LOG.info(" done.");
		
		//Fail if the cluster is not healthy.
		ClusterHealthResponse health = client.admin().cluster().health(new ClusterHealthRequest()).actionGet();
		LOG.info("Health :" + health.getStatus());
		if (health.getStatus().value() != 0) {
			throw new RuntimeException("Cluster is not healthy!");
		}
		
		return client;
	}
	
	public void beginCommit(Long txid) {
		LOG.debug("Beginning transaction for id:" + txid);
	}

	public void commit(Long txid) {
		LOG.debug("Committing transaction for id:" + txid);
	}

	public void updateBulk(List<String> keys, List<String> values) {

		BulkRequestBuilder bulkRequest = _client.prepareBulk();

		for (int j = 0; j < keys.size(); j++) {
			
			bulkRequest.add(
					_client.prepareIndex(_index, _type, keys.get(j)) 
					.setSource(values.get(j))
					);
		}

		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			for (BulkItemResponse resp : bulkResponse) {
				LOG.error(resp.getFailureMessage());
			}
			// process failures by iterating through each bulk response item
			// What to do here ? raise exception or gobble.
		}
	}
	
	public List<String> queryBulk(List<String> matchQueries) {
		MultiGetRequestBuilder request = _client.prepareMultiGet();
		if (matchQueries.size() == 0) {
			return new ArrayList<String>(0);
		}
		for (String key : matchQueries) {
			request.add(_index, _type, key);
		}

		MultiGetResponse results;
		try {
			results = request.execute().actionGet();
		} catch (ElasticSearchException e) {
			throw new RuntimeException(e);
		}

		List<String> ret = new ArrayList<String>(matchQueries.size());

		for (MultiGetItemResponse response : results) {
			GetResponse resp = response.getResponse();
			if (resp != null) {
				try {
					System.out.println("ES query result:" + resp.getSourceAsString());
					ret.add(resp.getSourceAsString());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			} else {
				ret.add(null);
			}
		}

		return ret;

	}


	public static class ESStateFactory implements StateFactory {

		private List<InetSocketTransportAddress> _s;
		private String _c;
		private String _i;
		private String _t;

		public ESStateFactory(List<InetSocketTransportAddress> servers, String clusterName, String index, String type) {
			_s = servers;
			_c = clusterName;
			_i = index;
			_t = type;
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new ESState(_s, _c, _i, _t);
		}
	}

}
