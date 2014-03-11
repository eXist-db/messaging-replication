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
import org.exist.memtree.MemTreeBuilder;
import org.exist.messaging.shared.ReportItem.CONTEXT;


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
    private List<ReportItem> errors = new ArrayList<ReportItem>();

    /**
     * Increase the nr of total received messages
     */
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

    /**
     * Increase the number of correctly processed messages
     */
    public void incMessageCounterOK() {
        messageCounterOK++;
    }

    /**
     * @return Total number of successfully received messages
     */
    public long getMessageCounterOK() {
        return messageCounterOK;
    }

    /**
     * Add the current processing time to the total processing time
     */
    public void addCumulatedProcessingTime() {
        this.totalTime += (stopTime - startTime);
    }
    /**
     * @return Total processing time
     */
    public long getCumulatedProcessingTime() {
        return totalTime;
    }

    /**
     * Add an Listener error to the report
     *
     * @param error The Listener error
     */
    public void addListenerError(Throwable error) {
        errors.add(new ReportItem(error, CONTEXT.LISTENER));
    }

    /**
     * Add an Receiver error
     *
     * @param error The Receiver error
     */
    public void addReceiverError(Throwable error) {
        errors.add(new ReportItem(error, CONTEXT.RECEIVER));
    }

    /**
     * Add an Connection error
     *
     * @param error The connection error
     */
    public void addConnectionError(Throwable error) {
        errors.add(new ReportItem(error, CONTEXT.CONNECTION));
    }

    /**
     * @return List tests of all problems
     */
    public List<String> getErrorMessages() {
        List<String> errorMessages = new ArrayList<String>();
        for (ReportItem t : errors) {
            errorMessages.add(t.getMessage());
        }
        return errorMessages;
    }

    /**
     * Get all report items
     *
     * @return All report items
     */
    public final List<ReportItem> getReportItems() {
        return errors;
    }

    /**
     * Set start time
     */
    public void start() {
        startTime = System.currentTimeMillis();
    }

    /**
     * Set stop time
     */
    public void stop() {
        stopTime = System.currentTimeMillis();
    }

    /**
     * Write all error messages to XML report.
     *
     * @param builder The builder to create the XML report.
     */
    public void write(MemTreeBuilder builder) {

        builder.startElement("", "errorMessages", "errorMessages", null);

        List<ReportItem> listenerErrors = getReportItems();
        if (!listenerErrors.isEmpty()) {
            for (ReportItem ri : listenerErrors) {
                ri.writeError(builder);
            }
        }

        builder.endElement();
    }
}
