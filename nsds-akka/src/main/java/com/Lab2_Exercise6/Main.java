package com.Lab2_Exercise6;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;

public class Main {
	
	public static void main(String[] args) throws Exception {
		ActorSystem sys = ActorSystem.create("MessagingApp");
		ActorRef server = sys.actorOf(Server.props(), "server");	
		ActorRef client1 = sys.actorOf(Client.props(server), "client1");
		ActorRef client2 = sys.actorOf(Client.props(server), "client2");
		
		client1.tell(new TextMsg("Hello World!"), ActorRef.noSender());
		client2.tell(new TextMsg("Hello Server!"), ActorRef.noSender());
		
		System.out.println("Wait for processing. Press enter afterwards to exit.");
		System.in.read();
		sys.terminate();
	}
}
