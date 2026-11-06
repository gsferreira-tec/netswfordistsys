package com.Comms_ex5;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;

public class Main {

	public static void main(String[] args) throws Exception {
		ActorSystem sys = ActorSystem.create("Contacts");
		ActorRef server = sys.actorOf(FaultSupervisor.props(), "supervisor");
		ActorRef client = sys.actorOf(Client.props(server), "client");
		
		client.tell(new PutMsg("Fernando", "fernando.oliveira@polimi.it"), ActorRef.noSender());
		client.tell(new PutMsg("Fabrizio", "fabrizio.lavezzi@polimi.it"), ActorRef.noSender());
		client.tell(new PutMsg("Juan", "juan.riveras@polimi.it"), ActorRef.noSender());
		client.tell(new GetMsg("Fernando"), ActorRef.noSender());
		client.tell(new PutMsg("Fail!", "anonymous@polimi.it"), ActorRef.noSender());
		client.tell(new GetMsg("Fabrizio"), ActorRef.noSender());
		client.tell(new GetMsg("Juan"), ActorRef.noSender());
		
		System.in.read();
		sys.terminate();
	}
}
