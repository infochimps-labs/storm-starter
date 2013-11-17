package storm.starter.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.infochimps.storm.testrig.util.VoterDB;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;

public class VoterState implements IBackingMap<OpaqueValue> {

//	private VoterDB _db;
	
	private ConcurrentHashMap<String, String> _voterDB = new ConcurrentHashMap<String, String>();
private JSONOpaqueSerializer _ser;

	public VoterState() {
//		_db = new VoterDB();
//		_db.initialize();

		_ser = new JSONOpaqueSerializer();
	}

	@Override
	public List<OpaqueValue> multiGet(List<List<Object>> keys) {
		System.out.println("Calling multiget with " + keys);
		List<OpaqueValue> gets = new ArrayList<OpaqueValue>();
		for (List<Object> key : keys) {
			String tmp = key.get(0).toString();
			System.out.println("Getting key:"+tmp+"from db:" +_voterDB);
			System.out.println(_voterDB.containsKey(tmp));
			
			// Get voter using query.
			if(!_voterDB.containsKey(tmp)){
				gets.add(null);
			}else{
				System.out.println("Getting value ["+(String)_voterDB.get(tmp)+"]key:"+tmp);
				gets.add(_ser.deserialize(_voterDB.get(tmp).getBytes()));
			}
			System.out.println(gets);
		}
		return gets;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<OpaqueValue> vals) {
		System.out.println("Calling multiput");
		for (int i = 0; i < keys.size(); i++) {
			String key = keys.get(i).get(0).toString();
			OpaqueValue val = vals.get(i);
			System.out.println(val);
			String serializedValue = new String(_ser.serialize(val));
			_voterDB.put(key, serializedValue);
			System.out.println("Putting key:"+key+" Value:"+serializedValue);
		}
		System.out.println(_voterDB);
	}

	public static class Factory implements StateFactory {
		StateType _type;

		public Factory() {

		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			// TODO Auto-generated method stub
			VoterState s = new VoterState();
			CachedMap c = new CachedMap(s, 1);
			MapState ms = OpaqueMap.build(s);
			return new SnapshottableMap(ms, new Values("$GLOBAL$"));
		}
	}

	

}
