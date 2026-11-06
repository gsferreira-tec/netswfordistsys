package com.evaluation23;

import java.util.concurrent.ThreadLocalRandom;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class TemperatureSensorActor extends AbstractActor {

	private ActorRef dispatcher;
	private final static int MIN_TEMP = 0;
	private final static int MAX_TEMP = 50;

	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				.match(GenerateMsg.class, this::onGenerate)
				.match(DispatcherRefMsg.class, this::onDispatcher) // matching dispatcher message to the related handler method
				.build();
	}

	private void onGenerate(GenerateMsg msg) {
		System.out.println("TEMPERATURE SENSOR: Sensing temperature!");
		int temp = ThreadLocalRandom.current().nextInt(MIN_TEMP, MAX_TEMP + 1);
		dispatcher.tell(new TemperatureMsg(temp,self()), self());
	}

	// needed to set the dispatcher for each sensor - added
	private void onDispatcher(DispatcherRefMsg msg) {
		dispatcher = msg.getDispatcher();
	}

	static Props props() {
		return Props.create(TemperatureSensorActor.class);
	}

}
