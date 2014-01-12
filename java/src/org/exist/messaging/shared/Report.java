/* 
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2014 The eXist Project
 *  http://exist-db.org 
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
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
