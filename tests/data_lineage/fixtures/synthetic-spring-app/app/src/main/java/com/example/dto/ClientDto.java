package com.example.dto;

@lombok.Data
public class ClientDto {
    private Long id;
    @JsonProperty("user_email") private String email;
    private String status;
}
