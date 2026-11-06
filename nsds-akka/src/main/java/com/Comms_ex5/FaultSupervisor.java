package com.Comms_ex5;

import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;

public class FaultSupervisor extends AbstractActor {
	
	private final ActorRef server = getContext().actorOf(ServerActor.props(), "props");
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(PutMsg.class, msg -> server.forward(msg, getContext()))
				.match(GetMsg.class, msg -> server.forward(msg, getContext()))
				.build();
	}
	
	@Override
	public SupervisorStrategy supervisorStrategy() {
		return new OneForOneStrategy(
				10,
				java.time.Duration.ofMinutes(1),
				DeciderBuilder
					.match(RuntimeException.class,
						ex -> SupervisorStrategy.resume())
					.matchAny(ex -> SupervisorStrategy.escalate())
					.build()
		);
	}
	
//	@Override
//	public SupervisorStrategy supervisorStrategy() {
//		return new OneForOneStrategy(
//				10,
//				java.time.Duration.ofMinutes(1),
//				DeciderBuilder
//					.match(RuntimeException.class,
//						ex -> SupervisorStrategy.restart())
//					.matchAny(ex -> SupervisorStrategy.escalate())
//					.build()
//		);
//	}
	
	public static Props props() {
		return Props.create(FaultSupervisor.class);
	}
}
