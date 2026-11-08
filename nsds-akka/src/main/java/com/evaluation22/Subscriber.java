package com.evaluation22;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;

public class Subscriber extends AbstractActor {

    private final ActorRef broker;

    @Override 
    public Receive createReceive() {
        return receiveBuilder()
            .match(NotifyMsg.class, this::onNotification) 
            .match(SubscribeMsg.class, this::onSubscribe)
            .build();
    }

    private Subscriber(ActorRef broker) {
        this.broker = broker;
    }

    public void onNotification(NotifyMsg m) {
        if(m != null) {
            System.out.println("Mew msg: " + m.getValue());
        } else {
            throw new RuntimeException("[ERROR] Notification msg is null.");
        }
    }

    public void onSubscribe(SubscribeMsg m) {
        broker.tell(m, getSelf());
    }

    public static Props props(ActorRef broker) {
        return Props.create(Subscriber.class,  () -> new Subscriber(broker));
    }
}

