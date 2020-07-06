package com.thehecklers.ejemploredis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@SpringBootApplication
@EnableRedisRepositories
public class EjemploRedisApplication {
    @Bean
    public RedisConnectionFactory connectionFactory() {
        return new JedisConnectionFactory();
    }

    public static void main(String[] args) {
        SpringApplication.run(EjemploRedisApplication.class, args);
    }

}

@EnableScheduling
@Component
class PollPlaneFinder {
    private WebClient client = WebClient.create("http://localhost:8080");

    private final RedisConnectionFactory connectionFactory;
    private final RedisConnection connection;
    private final RedisTemplate<String, String> template;
    private final AircraftRepository repository;

    PollPlaneFinder(RedisConnectionFactory connectionFactory, RedisTemplate<String, String> template, AircraftRepository repository) {
        this.connectionFactory = connectionFactory;
        this.connection = connectionFactory.getConnection();
        this.template = template;
        this.repository = repository;

        connection.serverCommands().flushDb();
    }

    @Scheduled(fixedRate = 1000)
    private void pollPlanes() {
        client.get()
                .retrieve()
                .bodyToFlux(Aircraft.class)
                .filter(plane -> !plane.getReg().isEmpty())
                .toStream()
                .forEach(ac -> repository.save(ac));

        repository.findAll().forEach(System.out::println);
    }
}

// MH: Toggle (comment out/in) above PollPlaneFinder class & this one to use repos or templates
// MH: Yes, I know it's crude, it's a QnD WIP ;)
/*@EnableScheduling
@Component
class PollPlaneFinder {
    private WebClient client = WebClient.create("http://localhost:8080");

    private final RedisConnectionFactory connectionFactory;
    private final RedisConnection connection;
    private final RedisTemplate<String, String> template;

    PollPlaneFinder(RedisConnectionFactory connectionFactory, RedisTemplate<String, String> template) {
        this.connectionFactory = connectionFactory;
        this.connection = connectionFactory.getConnection();
        this.template = template;

        connection.serverCommands().flushDb();
    }

    @Scheduled(fixedRate = 1000)
    private void pollPlanes() {
        client.get()
                .retrieve()
                .bodyToFlux(Aircraft.class)
                .filter(plane -> !plane.getReg().isEmpty())
                .toStream()
                .forEach(ac -> template.opsForValue().set(ac.getReg(), ac.toString()));

        template.opsForValue()
                .getOperations()
                .keys("*")
                .forEach(ac -> System.out.println(template.opsForValue().get(ac)));
    }
}*/
