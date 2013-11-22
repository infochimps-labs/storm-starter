package storm.starter.trident;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
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

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@SuppressWarnings({ "rawtypes", "serial", "unchecked" })
public class ElasticSearchState<T> implements IBackingMap<T> {
	private static final Map<StateType, Serializer> DEFAULT_SERIALIZERS = new HashMap<StateType, Serializer>() {
		{
			put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
			put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
			put(StateType.OPAQUE, new JSONOpaqueSerializer());
		}
	};

	public static class Options<T> implements Serializable {
		public int localCacheSize = 131072;
		public String globalKey = "$GLOBAL$";
		public Serializer<T> serializer = null;
		public String index = null;
		public String type = null;
		public String clusterName = "elasticsearch";
		public List<String> persistedKeys = null;
		public String indexSuffix = "_current";
	}

	public static StateFactory opaque(List<InetSocketTransportAddress> servers) {
		return opaque(servers, new Options<OpaqueValue>());
	}

	public static StateFactory opaque(List<InetSocketTransportAddress> servers, Options<OpaqueValue> opts) {
		return new Factory(servers, StateType.OPAQUE, opts);
	}

	public static StateFactory transactional(List<InetSocketTransportAddress> servers) {
		return transactional(servers, new Options<TransactionalValue>());
	}

	public static StateFactory transactional(List<InetSocketTransportAddress> servers, Options<TransactionalValue> opts) {
		return new Factory(servers, StateType.TRANSACTIONAL, opts);
	}

	public static StateFactory nonTransactional(List<InetSocketTransportAddress> servers) {
		return nonTransactional(servers, new Options<Object>());
	}

	public static StateFactory nonTransactional(List<InetSocketTransportAddress> servers, Options<Object> opts) {
		return new Factory(servers, StateType.NON_TRANSACTIONAL, opts);
	}

	protected static class Factory implements StateFactory {
		StateType _type;
		List<InetSocketTransportAddress> _servers;
		Serializer _ser;
		Options _opts;

		public Factory(List<InetSocketTransportAddress> servers, StateType type, Options options) {
			_type = type;
			_servers = servers;
			_opts = options;
			if (options.serializer == null) {
				_ser = DEFAULT_SERIALIZERS.get(type);
				if (_ser == null) {
					throw new RuntimeException("Couldn't find serializer for state type: " + type);
				}
				LOG.debug("Serialization defaulting to " + _ser.getClass().toString() + " for " + type.toString());
			} else {
				_ser = options.serializer;
			}
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			ElasticSearchState c;
			try {
				c = new ElasticSearchState(makeElasticSearchClient(_opts, _servers), _opts, _ser);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			MapState ms;
			// CachedMap c = new CachedMap(s, _opts.localCacheSize);
			if (_type == StateType.NON_TRANSACTIONAL) {
				ms = NonTransactionalMap.build(c);
			} else if (_type == StateType.OPAQUE) {
				ms = OpaqueMap.build(c);
			} else if (_type == StateType.TRANSACTIONAL) {
				ms = TransactionalMap.build(c);
			} else {
				throw new RuntimeException("Unknown state type: " + _type);
			}
			return new SnapshottableMap(ms, new Values(_opts.globalKey));
		}

		private Client makeElasticSearchClient(Options opts, List<InetSocketTransportAddress> endpoints) throws UnknownHostException {
			LOG.info("Creating client ....");
			Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", opts.clusterName).build();

			TransportClient client = new TransportClient(settings);
			for (InetSocketTransportAddress address : endpoints) {
				client.addTransportAddress(address);
			}

			LOG.info(" done.");
			// Fail if the cluster isn't healthy
			ClusterHealthResponse health = client.admin().cluster().health(new ClusterHealthRequest()).actionGet();
			LOG.info("Server reports it's " + health.getStatus());
			if (health.getStatus().value() != 0) {
				 throw new ElasticSearchException("Cluster is not healthy!");
			}
			return client;
		}

	}

	public static final Logger LOG = Logger.getLogger(ElasticSearchState.class);
	private final Client _client;
	private Options _opts;
	private Serializer<T> _ser;

	public ElasticSearchState(Client client, Options opts, Serializer<T> ser) {
		_client = client;
		_opts = opts;
		_ser = ser;
	}

	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		MultiGetRequestBuilder request = _client.prepareMultiGet();

		System.out.println("multiGet:" + keys);
		if (keys.size() == 0) {
			return new ArrayList<T>(0);
		}

		for (List<Object> key : keys) {
			String esIndex = _opts.index;
			String esType = _opts.type;
			String esId = (String) key.get(0);
			request.add(esIndex, esType, esId);
		}

		MultiGetResponse results;
		try {
			results = request.execute().actionGet();
		} catch (ElasticSearchException e) {
			throw e;
		}
		List<T> ret = new ArrayList<T>(keys.size());

		for (MultiGetItemResponse response : results) {
			GetResponse resp = response.getResponse();
			if (resp != null) {
				try {
					ObjectMapper mapper = new ObjectMapper();
					System.out.println(resp.getSourceAsString());
					JsonNode jsonNode = mapper.readValue(resp.getSourceAsString(), JsonNode.class);
					T val = (T) new OpaqueValue(jsonNode.findValue("currTxid").asLong(), jsonNode.findValue("prev").toString(), jsonNode.findValue("curr").toString());
					System.out.println("REBORN:" + val);
					ret.add(val);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			} else {
				ret.add(null);
			}
		}

		return ret;
	}

	@Override
	public void multiPut(List<List<Object>> groupByKeys, List<T> aggVals) {

		BulkRequestBuilder bulkRequest = _client.prepareBulk();

		for (int i = 0; i < groupByKeys.size(); i++) {
			T agg = aggVals.get(i);
			List<Object> keys = groupByKeys.get(i);
			String esIndex = _opts.index;
			String esType = _opts.type;
			String esId = (String) keys.get(0);

			ObjectMapper mapper = new ObjectMapper();
			try {
				String tmp = mapper.writeValueAsString(agg);
				System.out.println("Putting key:" + esId + " value:" + tmp);
				bulkRequest.add(_client.prepareIndex(esIndex, esType, esId).setSource(tmp));
			} catch (Exception ioEx) {
				throw new RuntimeException("Failed to encode object as JSON!", ioEx);
			}
		}

		BulkResponse result = bulkRequest.execute().actionGet();
		if (result.hasFailures()) {
			throw new RuntimeException(result.buildFailureMessage());
		}
	}

}