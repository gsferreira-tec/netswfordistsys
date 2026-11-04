package com.Comms;

import akka.actor.AbstractActor;

public class Client extends AbstractActor {
	
	public Receive createReceive () {
		return receiveBuilder()
				.match(null, null)
	};
	
}
