package com.evaluation22;

import akka.actor.AbstractActor;
import akka.actor.Props;
import java.util.HashMap;
import java.util.Map;
import akka.actor.ActorRef;

public class Worker extends AbstractActor {

    private final static String TOPIC0 = "topic0";
	private final static String TOPIC1 = "topic1";
    private final Map<String, ActorRef> sub2topic = new HashMap<>();

    @Override 
    public Receive createReceive() {
        return receiveBuilder()
            .match(PublishMsg.class, this::onPublish)
            .match(SubscribeMsg.class, this::onSubscribe)
            .build();
    }

    public void onPublish(PublishMsg m) {
        if(!m.getTopic().equals(TOPIC0) && !m.getTopic().equals(TOPIC1)) {
            throw new RuntimeException("[ERROR] Topic not recognized. Could not publish the message.");
        } else {
            ActorRef subscriberLookup = sub2topic.get(m.getTopic()); // returns the subscriber to the topic
            if(subscriberLookup != null) {
                subscriberLookup.tell(new NotifyMsg(m.getValue()), getSelf()); // send the notify message to that subscriber with the value of published msg
            } else throw new RuntimeException("There is no subscriber to " + m.getTopic());
        } 
    }

    public void onSubscribe(SubscribeMsg m) {
        sub2topic.put(m.getTopic(), m.getSender());
    }

    public static Props props() {
        return Props.create(Worker.class);
    }
}
