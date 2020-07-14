package com.thehecklers.ejemploredis;

import org.springframework.data.repository.CrudRepository;

public interface AircraftRepository extends CrudRepository<Aircraft, String> {
}

