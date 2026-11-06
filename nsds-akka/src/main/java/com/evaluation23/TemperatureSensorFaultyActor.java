package com.evaluation23;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class TemperatureSensorFaultyActor extends TemperatureSensorActor {

	private ActorRef dispatcher;
	private final static int FAULT_TEMP = -50;

	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				.match(GenerateMsg.class, this::onGenerate)
				.match(DispatcherRefMsg.class, this::onDispatcher) // matching dispatcher message to the related handler method here as well
				.build();
	}

	private void onGenerate(GenerateMsg msg) {
		System.out.println("TEMPERATURE SENSOR "+self()+": Sensing temperature!");
		dispatcher.tell(new TemperatureMsg(FAULT_TEMP,self()), self());
	}

	private void onDispatcher(DispatcherRefMsg msg) {
		dispatcher = msg.getDispatcher();
	}

	static Props props() {
		return Props.create(TemperatureSensorFaultyActor.class);
	}

}
