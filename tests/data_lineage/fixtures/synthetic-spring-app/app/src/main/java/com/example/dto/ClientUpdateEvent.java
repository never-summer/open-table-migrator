package com.example.dto;

@lombok.Data
public class ClientUpdateEvent {
    private Long id;
    @JsonProperty("user_email") private String email;
}
