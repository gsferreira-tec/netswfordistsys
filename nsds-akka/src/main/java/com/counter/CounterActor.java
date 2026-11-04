package com.counter;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

public class CounterActor extends AbstractActorWithStash {

	private int counter;

	public CounterActor() {
		this.counter = 0;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(SimpleMessage.class, this::onMessage).match(NewMessage.class, this::offMessage).build();
	}

	void onMessage(SimpleMessage msg) {
		++counter;
		unstashAll();
		System.out.println("Counter increased to " + counter);
	}
	
	void offMessage(NewMessage msg) {
		if(counter == 0) {
			stash();			
			//System.out.println("Counter is zero - cannot go negative " + counter);
			return;
		}
		counter--;
		System.out.println("Counter decreased to " + counter);
	}

	static Props props() {
		return Props.create(CounterActor.class);
	}

}
