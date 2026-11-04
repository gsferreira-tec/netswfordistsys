package com.Comms;

public class PutMsg {
	private final String name;
	private final String address;
	
	public PutMsg(String name, String address) {
		this.name = name;
		this.address = address;
	};
	
	public String getName() {
		return name;
	};
	
	public String getAddress() {
		return address;
	};
}
