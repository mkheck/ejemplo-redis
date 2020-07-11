package com.thehecklers.ejemploredis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@SpringBootApplication
@EnableRedisRepositories
public class EjemploRedisApplication {
    @Bean
    public RedisOperations<String, Aircraft> redisOperations(RedisConnectionFactory factory) {
        Jackson2JsonRedisSerializer<Aircraft> serializer = new Jackson2JsonRedisSerializer<>(Aircraft.class);

        RedisTemplate<String, Aircraft> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);
        template.setDefaultSerializer(serializer);
        template.setKeySerializer(new StringRedisSerializer());
        System.out.println("Default serializer? " + template.isEnableDefaultSerializer());

        return template;
    }

    public static void main(String[] args) {
        SpringApplication.run(EjemploRedisApplication.class, args);
    }
}

//@EnableScheduling
//@Component
//class PollPlaneFinder {
//    private WebClient client = WebClient.create("http://localhost:7634/aircraft");
//
//    private final RedisConnectionFactory connectionFactory;
//    private final AircraftRepository repository;
//
//    PollPlaneFinder(RedisConnectionFactory connectionFactory, AircraftRepository repository) {
//        this.connectionFactory = connectionFactory;
//        this.repository = repository;
//
//        connectionFactory.getConnection().serverCommands().flushDb();
//    }
//
//    @Scheduled(fixedRate = 1000)
//    private void pollPlanes() {
//        client.get()
//                .retrieve()
//                .bodyToFlux(Aircraft.class)
//                .filter(plane -> !plane.getReg().isEmpty())
//                .toStream()
//                .forEach(repository::save);
//
//        repository.findAll().forEach(System.out::println);
//    }
//}

// MH: Toggle (comment out/in) above PollPlaneFinder class & this one to use repos or templates
// MH: Yes, I know it's crude, it's a QnD WIP ;)
@EnableScheduling
@Component
class PollPlaneFinder {
    private WebClient client = WebClient.create("http://localhost:7634/aircraft");

    private final RedisConnectionFactory connectionFactory;
    private final RedisConnection connection;
    private final RedisOperations<String, Aircraft> redisOperations;

    PollPlaneFinder(RedisConnectionFactory connectionFactory, RedisOperations<String, Aircraft> redisOperations) {
        this.connectionFactory = connectionFactory;
        this.connection = connectionFactory.getConnection();
        this.redisOperations = redisOperations;

        connection.serverCommands().flushDb();
    }

    @Scheduled(fixedRate = 1000)
    private void pollPlanes() {
        client.get()
                .retrieve()
                .bodyToFlux(Aircraft.class)
                .filter(plane -> !plane.getReg().isEmpty())
                .toStream()
                .forEach(ac -> redisOperations.opsForValue().set(ac.getReg(), ac));

        redisOperations.opsForValue()
                .getOperations()
                .keys("*")
                .forEach(ac -> System.out.println(redisOperations.opsForValue().get(ac)));
    }
}
