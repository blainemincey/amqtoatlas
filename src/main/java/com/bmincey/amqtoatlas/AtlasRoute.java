package com.bmincey.amqtoatlas;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AtlasRoute extends RouteBuilder {

    private static final Logger logger = LoggerFactory.getLogger(AtlasRoute.class);

    @Autowired
    private MongoDBUtils mongoDBUtils;

    @Override
    public void configure() throws Exception {


        errorHandler(deadLetterChannel("{{outbound.deadletter.endpoint}}")
                .useOriginalMessage()
                .maximumRedeliveries(5)
                .retryAttemptedLogLevel(LoggingLevel.DEBUG)
                .redeliveryDelay(5000)
                .logStackTrace(true));

        from("{{inbound.endpoint}}")
                .transacted()
                .log(LoggingLevel.INFO, logger, "Processing Message in Queue.")
                .process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        String messageString = exchange.getIn().getBody(String.class);

                        logger.info("Message: {}", messageString);

                        Status status = JsonToJava.jsonToStatus(messageString);

                        logger.info(status.toString());

                        mongoDBUtils.process(status);
                    }
                })
                .log(LoggingLevel.INFO, logger, "Completed writing to MongoDB Atlas.");


    }
}
