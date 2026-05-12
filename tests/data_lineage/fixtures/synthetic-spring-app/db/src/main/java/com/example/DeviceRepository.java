package com.example;

public class DeviceRepository {
    public Object load(int id) {
        return jdbc.queryForObject(
            "SELECT id, mac, owner_id FROM devices WHERE id = ?",
            new Object[]{id}, deviceMapper);
    }
    public void touch(int id) {
        jdbc.update("UPDATE devices SET last_seen = NOW() WHERE id = ?", id);
    }
    public Object dynamic(String filter) {
        StringBuilder sb = new StringBuilder("SELECT * FROM devices WHERE ");
        sb.append(filter);
        return jdbc.query(sb.toString(), deviceMapper);
    }
}
