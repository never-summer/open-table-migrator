package com.example;

public class ClientService {
    public void publishUpdate(ClientDto dto) {
        ClientUpdateEvent ev = new ClientUpdateEvent();
        ev.id = dto.getId();
        ev.email = dto.getEmail();
        kafkaTemplate.send("client-updates", ev);
    }
    public void notifyExternal(ClientDto dto) {
        restClient.post().uri("/notify").body(dto).retrieve();
    }
}
