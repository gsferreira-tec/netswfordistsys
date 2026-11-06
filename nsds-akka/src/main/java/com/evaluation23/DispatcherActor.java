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
	private int logic = DispatchLogicMsg.LOAD_BALANCER;
	private final List<ActorRef> processors = new ArrayList<>();
	private final Map<ActorRef, ActorRef> assigned = new HashMap<>();
	private int next = 0;

	@Override 
	public void preStart() {
		for(int i = 0; i < NO_PROCESSORS; i++) {
			processors.add(getContext().actorOf(SensorProcessorActor.props(), "p" + i));
		}
	}

	// public DispatcherActor() {

	// }


	@Override
	public Receive createReceive() {
		return receiveBuilder()
			.match(TemperatureMsg.class, this::onTemperature)
			.match(DispatchLogicMsg.class, this::onLogic)
			.build();
	}

	private void onTemperature(TemperatureMsg m) {
		if(logic == DispatchLogicMsg.ROUND_ROBIN) {
			dispatchDataRoundRobin(m);
		} else {
			dispatchDataLoadBalancer(m);
		}
	}

	private void onLogic(DispatchLogicMsg m) {
		logic = m.getLogic();
	}

	private void dispatchDataLoadBalancer(TemperatureMsg msg) {
		ActorRef sensor = msg.getSender();
		ActorRef processor = assigned.get(sensor);
		if(processor == null) {
			processor = processors.get(0);
			int best = count(processor);
			for(ActorRef candidate : processors) {
				int n = count(candidate);
				if(n < best) {
					best = n;
					processor = candidate;
				}
			}
			assigned.put(sensor, processor);
		}
		processor.tell(msg, getSelf());
	}

	private void dispatchDataRoundRobin(TemperatureMsg msg) {
		// ...
		processors.get(next).tell(msg, getSelf());
		next = (next + 1) % processors.size();
	}

	private int count(ActorRef processor) {
		int n = 0;
		for(ActorRef dest : assigned.values()) {
			if(dest.equals(processor)) {
				n++;
			}
		}
		return n;
	}

	static Props props() {
		return Props.create(DispatcherActor.class);
	}
}
