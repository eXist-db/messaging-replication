
package org.exist.messaging.shared;

import java.util.ArrayList;
import java.util.List;


/**
 * Reporting class
 * 
 * @author Dannes Wessels
 */
public class Report {

    /*
     * Raw times
     */
    long startTime = -1;
    long stopTime = -1;

    /**
     * Number of messages
     */
    private long messageCounterOK = 0;
    private long messageCounterTotal = 0;
    /**
     * Cumulated time successful messages
     */
    private long totalTime = 0;
    /**
     * Storage for errors
     */
    private List<String> errors = new ArrayList<String>();

    public void incMessageCounterTotal() {
        messageCounterTotal++;
    }

    /**
     * @return Total number of received messages
     */
    public long getMessageCounterTotal() {
        return messageCounterTotal;
    }

    /**
     * @return Total number of NOT successfully received messages
     */
    public long getMessageCounterNOK() {
        return (messageCounterTotal - messageCounterOK);
    }

    public void incMessageCounterOK() {
        messageCounterOK++;
    }

    /**
     * @return Total number of successfully received messages
     */
    public long getMessageCounterOK() {
        return messageCounterOK;
    }

    public void addCumulatedProcessingTime() {
        this.totalTime += (stopTime - startTime);
    }
    /**
     * @return Total processing time
     */
    public long getCumulatedProcessingTime() {
        return totalTime;
    }

    public void add(String error) {
        errors.add(error);
    }

    /**
     * @return List of problems
     */
    public List<String> getErrors() {
        return errors;
    }

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void stop() {
        stopTime = System.currentTimeMillis();
    }
}
