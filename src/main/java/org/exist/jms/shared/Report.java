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

package org.exist.jms.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.dom.memtree.MemTreeBuilder;
import org.exist.jms.shared.ReportItem.CONTEXT;

import java.util.ArrayList;
import java.util.List;


/**
 * Reporting class
 *
 * @author Dannes Wessels
 */
public class Report {

    private final static Logger LOG = LogManager.getLogger(Report.class);

    /**
     * Storage for errors
     */
    private final List<ReportItem> errors = new ArrayList<>();
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
    public void addListenerError(final Throwable error) {
        errors.add(new ReportItem(error, CONTEXT.LISTENER));
    }

    /**
     * Add an Receiver error
     *
     * @param error The Receiver error
     */
    public void addReceiverError(final Throwable error) {
        errors.add(new ReportItem(error, CONTEXT.RECEIVER));
    }

    /**
     * Add an Connection error
     *
     * @param error The connection error
     */
    public void addConnectionError(final Throwable error) {
        errors.add(new ReportItem(error, CONTEXT.CONNECTION));
    }

    /**
     * @return List texts of all problems
     */
    public List<String> getErrorMessages() {
        final List<String> errorMessages = new ArrayList<>();
        errors.forEach((t) -> errorMessages.add(t.getMessage()));
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
    public void write(final MemTreeBuilder builder) {

        builder.startElement("", "errorMessages", "errorMessages", null);

        final List<ReportItem> listenerErrors = getReportItems();
        if (!listenerErrors.isEmpty()) {
            listenerErrors.forEach((ri) -> ri.writeError(builder));
        }

        builder.endElement();
    }

    public void clear() {
        LOG.info("Clear report");

        errors.clear();
        messageCounterOK = 0;
        messageCounterTotal = 0;

    }
}
