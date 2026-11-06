package com.Comms_ex5;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Client extends AbstractActor {
	
	private final ActorRef server;
	
	public Client(ActorRef server) {
		this.server = server;
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(PutMsg.class, this::onPut)
				.match(GetMsg.class, this::onGet)
				.match(RespMsg.class, this::onResp)
				.build();
	}
	
	void onPut(PutMsg m) {
		server.tell(m, getSelf());
	}
	
	void onGet(GetMsg m) {
		server.tell(m, getSelf());
	}
	
	void onResp(RespMsg m) {
		System.out.println("Address: " + m.getAddress());
	}
	
	static Props props(ActorRef server) {
		return Props.create(Client.class, server);
	}
}
