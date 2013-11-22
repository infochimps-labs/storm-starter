package storm.starter.trident;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.infochimps.storm.testrig.util.Voter;

public class WukongFunctionStub {
	
    static ObjectMapper mapper   = new ObjectMapper();

	public static String[] generateKey(String raw){
		return raw.split(";");
	}

	public static String generateQuery(String key, String content){
		return key;
	}
	
	public static String combine(String val1, String val2){
		return val1 +","+ val2;
	}

	public static String updateVoter(String key, String voter, String group) {
		Voter tmp = null;
		if(voter == null) {
			tmp = new Voter(key, new ArrayList<String>(), 0);
		}else{
			try {
				tmp = mapper.readValue(voter, Voter.class);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		String [] g = group.split(",");
		List<String> donations = tmp.getDonations();
		for (String string : g) {
			donations.add(string);
		}
		tmp.setDonations(donations);
		String res = "";
		try {
			res = mapper.writeValueAsString(tmp);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}
}
