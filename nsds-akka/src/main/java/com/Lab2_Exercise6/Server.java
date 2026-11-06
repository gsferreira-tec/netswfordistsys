package com.Lab2_Exercise6;

import akka.actor.*;



public class Server extends AbstractActorWithStash {
	
	private final int delay = 3;
	private boolean busy = false;
	private ActorRef currentClient;
	private String currentText;
	
	public static class ProcessingDone { }
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(TextMsg.class, this::onMessage)
				.match(ProcessingDone.class, this::onDone)
				.build();
	}
	
	public void onMessage(TextMsg m) {
		if(busy) {
			stash();
			return;
		}
		
		busy = true;
		currentClient = getSender();
		currentText = m.getTextMsg();
			
		System.out.println("Server received: " + currentText);
		
		getContext().system().scheduler().scheduleOnce(
				java.time.Duration.ofSeconds(delay), 
				getSelf(),
				new ProcessingDone(),
				getContext().dispatcher(),
				getSelf()
		);
	}
	
	private void onDone(ProcessingDone m) {
		
		System.out.println("Server finished: " + currentText.toUpperCase());
		currentClient.tell(currentText.toUpperCase(), getSelf());
		busy = false;
		unstashAll();
	}
	
	
	
	public static Props props() {
		return Props.create(Server.class);
	}
}

