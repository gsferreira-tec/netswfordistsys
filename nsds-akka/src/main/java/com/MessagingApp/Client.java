package com.MessagingApp;

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
				.match(SleepMsg.class, this::sendSleep)
				.match(WakeupMsg.class, this::sendWakeup)
				.match(TextMsg.class, this::sendText)
				.match(String.class, text -> System.out.println("Reply received: " + text))
				.build();
	}
	
	void sendSleep(SleepMsg m) {
		server.tell(m, getSelf());
	}
	
	void sendWakeup(WakeupMsg m) {
		server.tell(m, getSelf());
	}
	
	void sendText(TextMsg m) {
		server.tell(m, getSelf());
	}
	
	static Props props(ActorRef server) {
		return Props.create(Client.class, server);
	}
}
