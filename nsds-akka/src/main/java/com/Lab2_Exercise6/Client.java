package com.Lab2_Exercise6;

import akka.actor.*;

public class Client extends AbstractActor {
	
	private final ActorRef server;
	
	public Client(ActorRef server ) {
		this.server = server;
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(TextMsg.class, this::sendMessage)
				.match(String.class, reply -> System.out.println(getSelf().path().name() + " received: " + reply))
				.build();
	}
	
	void sendMessage(TextMsg m) {
		server.tell(m, getSelf());
	}
	
	static Props props(ActorRef server) {
		return Props.create(Client.class, server);
	}
}
