package com.example.demo.repository;

import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class DemoRepository<K, V> {

    private final Map<K, V> dataMap = new ConcurrentHashMap<>();

    public V get(K key) {
        return dataMap.get(key);
    }

    public V save(K key, V value) {
        return dataMap.put(key, value);
    }
}
