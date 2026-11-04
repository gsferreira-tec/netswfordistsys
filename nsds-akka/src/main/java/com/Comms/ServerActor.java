package com.Comms;

import java.util.Map;
import java.util.HashMap;
import akka.actor.AbstractActor;
import akka.actor.Props;

public class ServerActor extends AbstractActor {

	private final Map<String, String> contacts = new HashMap<>();

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(PutMsg.class, this::onPut)
				.match(GetMsg.class, this::onGet)
				.build();
	}

	void onPut(PutMsg m) {
		contacts.put(m.getName(), m.getAddress());
	}

	void onGet(GetMsg m) {
		getSender().tell(new RespMsg(contacts.get(m.getName())), getSelf());
	}

	static Props props() {
		return Props.create(ServerActor.class);
	}
}
