package com.evaluation23;

import akka.actor.ActorRef;

public class DispatcherRefMsg {
    private final ActorRef dispatcher;

    public DispatcherRefMsg(ActorRef dispatcher) {
        this.dispatcher = dispatcher;
    }

    public ActorRef getDispatcher() {
        return dispatcher;
    }
}
