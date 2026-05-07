package com.example;

@RestController
public class ClientApi {
    @GetMapping("/clients/{id}")
    public ClientDto get(@PathVariable Long id) { return null; }

    @PostMapping("/clients")
    public ClientDto create(@RequestBody ClientDto in) { return null; }
}
