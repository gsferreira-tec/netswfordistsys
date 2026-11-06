package com.evaluation23;

import akka.actor.ActorRef;
// new type of message to give the dispatcher reference to the sensors
public class DispatcherRefMsg {
    private final ActorRef dispatcher;

    // setting the private field to the dispatcher passsed via arg in main
    public DispatcherRefMsg(ActorRef dispatcher) {
        this.dispatcher = dispatcher;
    }

    // for the sensor to be able to get the reference in TemperatureSensorActor and TemperatureSensorFaultyActor
    public ActorRef getDispatcher() {
        return dispatcher;
    }
}
