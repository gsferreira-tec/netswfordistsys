package com.MessagingApp;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

public class Server extends AbstractActorWithStash{
		
	private boolean sleeping = false;
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(SleepMsg.class, this::onSleep)
				.match(WakeupMsg.class, this::onWakeup)
				.match(TextMsg.class, this::onText)
				.build();
	}
	
	void onSleep(SleepMsg m) {
		sleeping = true;
		System.out.println("Stashed: " + m.getSleepMsg() + "\n" + "Going to sleep...");
	}
	
	void onWakeup(WakeupMsg m) {
		sleeping = false; 
		unstashAll();
		System.out.println("Waking back up...");
	}
	
	void onText(TextMsg m) {
		if(sleeping) {
			stash();
		} else {			
			getSender().tell(m.getTextMsg(), getSelf());
		}
	}
	
	static Props props() {
		return Props.create(Server.class);
	}
}
