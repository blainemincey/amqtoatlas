package com.bmincey.amqtoatlas;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;


public class Status implements Serializable {

    private static final String INTERNET = "INTERNET";
    private static final String NETWORK = "NETWORK";
    private static final String UP = "UP";
    private static final String DOWN = "DOWN";

    private String networkType;
    private String status;
    private String dateTime;

    /**
     *
     */
    public Status() {

    }

    /**
     * @param networkType
     * @param status
     * @param dateTime
     */
    public Status(final String networkType, final String status, final String dateTime) {
        this.setNetworkType(networkType);
        this.setStatus(status);
        this.setDateTime(dateTime);

    }

    /**
     * @return
     */
    public String getNetworkType() {
        return networkType;
    }

    /**
     * @param networkType
     */
    public void setNetworkType(String networkType) {
        this.networkType = networkType;
    }

    /**
     * @return
     */
    public String getStatus() {
        return status;
    }

    /**
     * @param status
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * @return
     */
    public String getDateTime() {
        return dateTime;
    }

    /**
     * @param dateTime
     */
    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    @Override
    public String toString() {
        return "Status{" +
                "networkType='" + networkType + '\'' +
                ", status='" + status + '\'' +
                ", dateTime='" + dateTime + '\'' +
                '}';
    }

    /**
     * @return
     */
    public LocalDateTime getDateTimeStringAsLocalDateTime() {
        // 2017-11-15T13:20:02.224 -- ISO format
        LocalDateTime localDateTime = LocalDateTime.parse(getDateTime());

        return localDateTime;
    }


    public java.util.Date getDateTimeAsDate() {

        LocalDateTime localDateTime = this.getDateTimeStringAsLocalDateTime();

        Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
        java.util.Date date = java.util.Date.from(instant);
        return date;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Status status = new Status(Status.NETWORK, Status.UP, LocalDateTime.now() + "");
        System.out.println(status);

        System.out.println(status.getDateTimeStringAsLocalDateTime().toString());
    }
}
