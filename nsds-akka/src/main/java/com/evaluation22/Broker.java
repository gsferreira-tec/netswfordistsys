package com.evaluation22;

import akka.actor.Props;
import akka.japi.pf.DeciderBuilder;

import java.time.Duration;

import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.SupervisorStrategy;

public class Broker extends AbstractActorWithStash {

    private boolean batch = false;
    private final ActorRef worker1 = getContext().actorOf(Worker.props(), "worker1");
    private final ActorRef worker2 = getContext().actorOf(Worker.props(), "worker2");


    @Override 
    public SupervisorStrategy supervisorStrategy() {
        return new OneForOneStrategy(
            1,
            Duration.ofMinutes(1),
            DeciderBuilder
                .match(Exception.class, e -> SupervisorStrategy.resume())
                .build()
        );
    }

    @Override 
    public Receive createReceive() {
        return receiveBuilder()
            .match(BatchMsg.class, this::onBatch)
            .match(PublishMsg.class, this::onEvent)
            .match(SubscribeMsg.class, this::onSubscribe)
            .build();
    }

    public void onBatch(BatchMsg m) {
        this.batch = m.isOn();
        if(!batch) {
            unstashAll();
        }
    }

    public void onEvent(PublishMsg m) {
        if(batch) {
            stash();
        } else {
            worker1.tell(m, getSelf());
            worker2.tell(m, getSelf());
        }
    }

    public void onSubscribe(SubscribeMsg m) {
        if(m.getKey() % 2 == 0) {
            worker1.tell(m, getSelf());
        } else {
            worker2.tell(m, getSelf());
        }
    }

    public static Props props() {
        return Props.create(Broker.class);
    }
}
