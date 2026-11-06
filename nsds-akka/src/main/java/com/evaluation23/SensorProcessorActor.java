package com.evaluation23;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class SensorProcessorActor extends AbstractActor {

	private double currentAverage;
	private int readings = 0;

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(TemperatureMsg.class, this::gotData).build();
	}

	private void gotData(TemperatureMsg msg) throws Exception {
		readings++;
		System.out.println("SENSOR PROCESSOR " + self() + ": Got data from " + msg.getSender());

		currentAverage = currentAverage + (msg.getTemperature() - currentAverage) / readings;

		if(currentAverage < 0) {
			throw new RuntimeException("[ERROR] Temperature avg dropped to negative!");
		}
		
		System.out.println("SENSOR PROCESSOR " + self() + ": Current avg is " + currentAverage);
	}

	static Props props() {
		return Props.create(SensorProcessorActor.class);
	}

	public SensorProcessorActor() {
	}
}
