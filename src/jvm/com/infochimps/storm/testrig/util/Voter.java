package com.infochimps.storm.testrig.util;

import java.util.List;

public class Voter {
	private String name;
	private List<String> donations;
	public List<String> getDonations() {
		return donations;
	}

	public void setDonations(List<String> donations) {
		this.donations = donations;
	}

	private int noOfDonations;
	
	
	public Voter(String name, List<String> donations, int noOfDonations) {
		this.name = name;
		this.donations = donations;
		this.noOfDonations = noOfDonations;
	}

	public int getNoOfDonations() {
		return noOfDonations;
	}

	public void setNoOfDonations(int noOfDonations) {
		this.noOfDonations = noOfDonations;
	}

	public Voter(){
		
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String toString(){
		return name + ":" + donations.toString();
	}

}
