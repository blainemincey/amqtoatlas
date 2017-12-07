package com.bmincey.amqtoatlas;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;

@Component
public class MongoDBConfig {


    @Value("${spring.data.mongodb.uri}")
    private MongoClientURI mongoClientURI;


    @Bean
    MongoClient mongoClient() throws UnknownHostException {
        return new MongoClient(mongoClientURI);
    }
}
