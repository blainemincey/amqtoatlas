package com.bmincey.amqtoatlas;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * @author bmincey (blaine.mincey@gmail.com)
 * <p>
 * Date Created: 11/15/17
 */
@Component
@PropertySource(value = "classpath:application.properties")
public class MongoDBUtils {

    @Autowired
    private MongoDBConfig mongoDBConfig;

    @Value("${spring.data.mongodb.database}")
    private String databaseName;

    @Value("${spring.data.mongodb.collection}")
    private String collectionName;

    MongoDatabase mongoDatabase;
    MongoCollection mongoCollection;

    private static final Logger logger = LoggerFactory.getLogger(MongoDBUtils.class);

    /**
     *
     */
    public MongoDBUtils() {

    }

    /**
     * @return
     */
    private void dbInit() {

        logger.info("DB init");

        try {
            mongoDatabase = mongoDBConfig.mongoClient().getDatabase(databaseName);
            mongoCollection = mongoDatabase.getCollection(collectionName);
        } catch (UnknownHostException uhe) {
            logger.error(uhe.getMessage());
        }
    }

    /**
     * @param status
     */
    public void process(Status status) {

        logger.info("Begin processing status: " + status);

        if (this.mongoCollection == null) {
            this.dbInit();
        }

        MaxDate maxDate = this.checkMaxDate(status);
        if (maxDate != null) {
            this.dateCheck(maxDate, status);
        } else {
            logger.info("MaxDate is null.  Initial insert for day.");
            this.insertStatus(status);
        }
    }

    /**
     * @param status
     * @return
     */
    private MaxDate checkMaxDate(Status status) {
        logger.info("CheckMaxDate for status object: " + status);

        java.util.Date timeStampDay = null;
        ObjectId objectId = null;

        MongoCursor<Document> cursor = this.mongoCollection.find().sort(new BasicDBObject("timeSeriesDay", -1)).limit(1).iterator();

        try {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                timeStampDay = document.getDate("timeSeriesDay");
                objectId = document.getObjectId("_id");
            }
        } finally {
            cursor.close();
        }

        LocalDate localDate = null;
        MaxDate maxDate = null;


        if (timeStampDay != null && objectId != null) {
            localDate = timeStampDay.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            maxDate = new MaxDate(objectId, localDate);
        } else {
            logger.error("TimeStampDay is null!");
        }

        return maxDate;
    }

    /**
     * @param maxDate
     * @param status
     */
    private void dateCheck(MaxDate maxDate, Status status) {
        // if now is equal to date in DB, update.
        // if now is greater than oldest date in DB, insert
        LocalDate localDate = status.getDateTimeStringAsLocalDateTime().toLocalDate();

        if (maxDate.getLocalDate().isBefore(localDate)) {
            this.insertStatus(status);
        } else {
            this.updateStatus(maxDate, status);
        }

    }

    private void insertStatus(Status status) {
        LocalDate localDate = status.getDateTimeStringAsLocalDateTime().toLocalDate();
        logger.info("Inserting data for " + localDate);

        java.util.Date date = java.util.Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());

        BasicDBList basicDBList = new BasicDBList();
        DBObject document = new BasicDBObject();
        document.put("timeStamp", new BsonDateTime(status.getDateTimeAsDate().getTime()));
        document.put("networkType", status.getNetworkType());
        document.put("status", status.getStatus());
        basicDBList.add(document);

        Document mainDoc = new Document().append("timeSeriesDay", new BsonDateTime(date.getTime()))
                .append("timeSeriesData", basicDBList);

        this.mongoCollection.insertOne(mainDoc);
    }

    /**
     * @param status
     * @return
     */
    private void updateStatus(MaxDate maxDate, Status status) {
        ObjectId objectId = maxDate.getObjectId();

        BasicDBList basicDBList = new BasicDBList();

        DBObject document = new BasicDBObject();
        document.put("timeStamp", new BsonDateTime(status.getDateTimeAsDate().getTime()));
        document.put("networkType", status.getNetworkType());
        document.put("status", status.getStatus());

        basicDBList.add(document);

        Document selectQuery = new Document("_id", objectId);
        Document updateQuery = new Document("$push", new Document("timeSeriesData", document));

        this.mongoCollection.updateOne(selectQuery, updateQuery);
    }

}
