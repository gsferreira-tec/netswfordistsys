package com.evaluation23;

import java.util.ArrayList;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithStash;
import akka.actor.Props;
import akka.actor.ActorRef;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class DispatcherActor extends AbstractActorWithStash {

	private final static int NO_PROCESSORS = 2;
	private int logic = DispatchLogicMsg.LOAD_BALANCER; // DEFAULT logic is LOAD_BALANCER... could be ROUND_ROBIN
	private final List<ActorRef> processors = new ArrayList<>(); // need the sensor processors list 
	private final Map<ActorRef, ActorRef> assigned = new HashMap<>(); // mapping sensors to processors
	private int next = 0;

	// getting the context from ActorSystem about the processors
	@Override 
	public void preStart() {
		for(int i = 0; i < NO_PROCESSORS; i++) {
			processors.add(getContext().actorOf(SensorProcessorActor.props(), "p" + i)); // gettign references from sys about the processors
		}
	}

	// not really needed here but could stay...
	// public DispatcherActor() {

	// }


	@Override
	public Receive createReceive() {
		return receiveBuilder()
			.match(TemperatureMsg.class, this::onTemperature)
			.match(DispatchLogicMsg.class, this::onLogic)
			.build();
	}

	// basing the method for handling the temperature dispatch on the logic coming from DispatchLogicMsg
	private void onTemperature(TemperatureMsg m) {
		if(logic == DispatchLogicMsg.ROUND_ROBIN) {
			dispatchDataRoundRobin(m);
		} else {
			dispatchDataLoadBalancer(m);
		}
	}

	// to set the private logiv field in DispatcherActor
	private void onLogic(DispatchLogicMsg m) {
		logic = m.getLogic();
	}

	// this implementation was a bit over my head - needs to keep track of the sensor that sent the data
	private void dispatchDataLoadBalancer(TemperatureMsg msg) {
		ActorRef sensor = msg.getSender();
		ActorRef processor = assigned.get(sensor); // setting the processor to the one attached to the sensor in "assigned"
		if(processor == null) { // if there are none
			processor = processors.get(0); // get processor with 0 sensors
			int best = count(processor); // start with the number of sensors attached to a processor
			for(ActorRef candidate : processors) {
				int n = count(candidate); // sample the # of sensors attached to processor "candidate"
				if(n < best) { // if the # of sensors is smaller than the initially considered then the current candidate is better
					best = n;
					processor = candidate; // set the processor to send the data to the current candidate
				}
			}
			assigned.put(sensor, processor); // in the end we assign the sensor to the processor with the least amount of sensors attached (n)
		}
		processor.tell(msg, getSelf()); // finally we send the data to the chosen processor
	}

	// implementation of ROUND_ROBIN which ignores from which sensor the reading came...
	// just forwards the msg to the next proc in processors and cycles next through the number 
	// of processors in the list
	private void dispatchDataRoundRobin(TemperatureMsg msg) {
		// ...
		processors.get(next).tell(msg, getSelf());
		next = (next + 1) % processors.size();
	}

	// counter for the number od sensors attached to a processor >> used in dispatchDataLoadBalancer()
	private int count(ActorRef processor) {
		int n = 0;
		for(ActorRef dest : assigned.values()) {
			if(dest.equals(processor)) { // if a sensor is attachet to the processor considered
				n++; // we increase the number of sensors counted to that processor
			}
		}
		return n;
	}

	static Props props() {
		return Props.create(DispatcherActor.class);
	}
}
