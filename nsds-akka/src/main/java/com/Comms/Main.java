package com.Comms;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;

public class Main {

	public static void main(String[] args) throws Exception {
		ActorSystem sys = ActorSystem.create("Contacts");
		ActorRef server = sys.actorOf(ServerActor.props(), "server");
		ActorRef client = sys.actorOf(Client.props(server), "client");
		
		client.tell(new PutMsg("Fernando", "fernando.oliveira@polimi.it"), ActorRef.noSender());
		client.tell(new PutMsg("Fabrizio", "fabrizio.lavezzi@polimi.it"), ActorRef.noSender());
		client.tell(new PutMsg("Juan", "juan.riveras@polimi.it"), ActorRef.noSender());
		client.tell(new GetMsg("Fernando"), ActorRef.noSender());
		client.tell(new GetMsg("Fabrizio"), ActorRef.noSender());
		client.tell(new GetMsg("Juan"), ActorRef.noSender());
		
		
		System.in.read();
		sys.terminate();
	}
}
