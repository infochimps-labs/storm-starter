package storm.starter.trident;

public class WukongFunctionStub {
	
	public static String[] generateKey(String raw){
		return raw.split(";");
	}

	public static String generateQuery(String key, String content){
		return "";
	}
	
	public static String combine(String voter, String emails){
		return voter + emails;
	}
}
