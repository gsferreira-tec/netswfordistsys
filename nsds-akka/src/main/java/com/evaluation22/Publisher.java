package com.evaluation22;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;

public class Publisher extends AbstractActor {

    private final ActorRef broker; // reference to the broker since it's the point of communication

    @Override 
    public Receive createReceive() {
        return receiveBuilder()
            .match(PublishMsg.class, this::onPublish)
            .build();
    }

    private Publisher(ActorRef broker) {
        this.broker = broker;
    }

    public void onPublish(PublishMsg m) {
        // send msg to broker??
        broker.tell(m, getSelf());
    }

    public static Props props(ActorRef broker) {
        return Props.create(Publisher.class, () -> new Publisher(broker));
    }
}
