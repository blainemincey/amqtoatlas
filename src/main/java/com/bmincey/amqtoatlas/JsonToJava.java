package com.bmincey.amqtoatlas;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonToJava {

    private static final Logger logger = LoggerFactory.getLogger(JsonToJava.class);

    /**
     * @param jsonString
     * @return
     */
    public static Status jsonToStatus(String jsonString) {
        Status status = null;

        ObjectMapper mapper = new ObjectMapper();

        logger.info("Convert JSON string: " + jsonString);

        try {
            status = mapper.readValue(jsonString, Status.class);
        } catch (JsonGenerationException jge) {
            logger.error(jge.getMessage());
        } catch (JsonMappingException jme) {
            logger.error(jme.getMessage());
        } catch (IOException ioe) {
            logger.error(ioe.getMessage());
        }

        return status;
    }

    public static void main(String[] args) {
        System.out.println("Starting...");

        String jsonString = "{\"networkType\":\"NETWORK\",\"status\":\"UP\",\"dateTime\":\"2017-11-14T19:45:01.631\"}";

        Status status = JsonToJava.jsonToStatus(jsonString);

        System.out.println(status);

        System.out.println("Ending.");
    }
}
