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

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

/**
 * Interface definition
 *
 * @author wessels
 */
public abstract class eXistMessagingListener implements MessageListener, ExceptionListener {

    private final static Logger LOG = LogManager.getLogger(eXistMessagingListener.class);


    private final Report report = new Report();
    private Session session;
    private int id = -1;

    /**
     * Get report of the JMS listener.
     *
     * @return The report
     */
    public Report getReport() {
        return report;
    }

    /**
     * Get they way the listener is used.
     *
     * @return Description how listener is used.
     */
    abstract public String getUsageType();

    public Session getSession() {
        return session;
    }

    /**
     * Set the JMS session so the listener can control the session.
     *
     * @param session The JMS session.
     */
    public void setSession(final Session session) {
        this.session = session;
    }

    public int getReceiverID() {
        return id;
    }

    /**
     * Set human-friendly identifier, handy for debugging.
     *
     * @param id Identifier
     */
    public void setReceiverID(final int id) {
        this.id = id;
    }

    @Override
    public void onException(final JMSException jmse) {

        getReport().addConnectionError(jmse);

        // Report exception
        final StringBuilder sb = new StringBuilder();
        sb.append(jmse.getMessage());

        final String txt = jmse.getErrorCode();
        if (txt != null) {
            sb.append(" (").append(txt).append(") ");
        }

        LOG.error(sb.toString(), jmse);

        // If there is a linked exception, report it too
        final Exception linkedException = jmse.getLinkedException();
        if (linkedException != null) {
            LOG.error("Linked with: " + linkedException.getMessage(), linkedException);
        }
    }

}
