package com.example;

import com.example.generated.tables.Clients;

public class ClientRepository {
    public Object findActive() {
        return dsl.select(CLIENTS.ID, CLIENTS.EMAIL).from(CLIENTS).fetch();
    }
    public void insertClient(String email) {
        dsl.insertInto(CLIENTS, CLIENTS.EMAIL).values(email).execute();
    }
    @Query("SELECT c FROM Client c WHERE c.email = :email")
    public Object findByEmail(String email) { return null; }
}
