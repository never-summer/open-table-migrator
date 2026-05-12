package com.example.kafka;

public class ClientUpdateListener {
    @KafkaListener(topics = "client-updates")
    public void onMsg(ClientUpdateEvent ev) {
    }
}
