package com.infochimps.storm.testrig.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.actors.threadpool.Arrays;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VoterDB {
	
	static ObjectMapper mapper = new ObjectMapper();

	
	private Map<String,Voter> voters = new HashMap<String,Voter>();
	
	
	public static String toJSON(Voter v) {
		try {
			return mapper.writeValueAsString(v);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "{Error: \"BOOOM!\"}";
	}
	
	public static Voter fromJSON(String v){
		try {
			return mapper.readValue(v, Voter.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null;
	}
	
	public String get(String key){
		Voter tmp = voters.get(key);
		return toJSON(tmp);		
	}

	public String update(String key, String voter){
		Voter tmp = fromJSON(voter);
		voters.put(key, tmp);
		return voter;
	}
	
	public void initialize(){
		for (int i = 0; i < 10; i++) {
			Voter tmp = new Voter("voter"+i, new ArrayList<String>());
			voters.put("voter"+i, tmp);
			System.out.println("Init: "+ tmp);
		}
	}
	
	public String toString(){
		return voters.toString();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		VoterDB db = new VoterDB();
		db.initialize();
		String vot1 = db.get("voter1");
		System.out.println(vot1);
		Voter vot1_ = VoterDB.fromJSON(vot1);
		vot1_.setEmails(Arrays.asList(new String[] {"as", "sdf", "dasdads"}));
		db.update("voter1", VoterDB.toJSON(vot1_));
		System.out.println(db);
	}

}
