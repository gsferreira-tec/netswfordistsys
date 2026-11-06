package com.MessagingApp;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;

public class Main{
	
	public static void main(String[] args) {
		ActorSystem sys = ActorSystem.create("Messenger");
		ActorRef server = sys.actorOf(Server.props(), "server");
		ActorRef client = sys.actorOf(Client.props(server), "client");
		
		client.tell(new TextMsg("Hello! How are you?"), ActorRef.noSender());
		client.tell(new TextMsg("I'm fine as well, thank you!"), ActorRef.noSender());
		client.tell(new SleepMsg("Go To Sleep Please!"), ActorRef.noSender());
		client.tell(new TextMsg("Are you going out today?"), ActorRef.noSender());
		client.tell(new TextMsg("I'm going to be studying for the NSDS evaluation lab on Monday... :("), ActorRef.noSender());
		client.tell(new WakeupMsg("Now wake back up please!"), ActorRef.noSender());

		sys.terminate();
	}
}
