package com.bmincey.amqtoatlas;

import org.bson.types.ObjectId;

import java.time.LocalDate;

/**
 * @author bmincey (blaine.mincey@gmail.com)
 * <p>
 * Date Created: 11/15/17
 */
public class MaxDate {

    private LocalDate localDate;
    private ObjectId objectId;

    /**
     * @param objectId
     * @param localDate
     */
    public MaxDate(ObjectId objectId, LocalDate localDate) {
        this.setLocalDate(localDate);
        this.setObjectId(objectId);
    }

    /**
     * @return
     */
    public LocalDate getLocalDate() {
        return localDate;
    }

    /**
     * @param localDate
     */
    public void setLocalDate(LocalDate localDate) {
        this.localDate = localDate;
    }

    /**
     * @return
     */
    public ObjectId getObjectId() {
        return objectId;
    }

    /**
     * @param objectId
     */
    public void setObjectId(ObjectId objectId) {
        this.objectId = objectId;
    }

    /**
     * @return
     */
    @Override
    public String toString() {
        return "MaxDate{" +
                "localDate=" + localDate +
                ", objectId=" + objectId +
                '}';
    }
}
