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

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;
import org.apache.log4j.Logger;

/**
 * Interface definition 
 * @author wessels
 */
public abstract class eXistMessagingListener implements MessageListener, ExceptionListener {

    private final static Logger LOG = Logger.getLogger(eXistMessagingListener.class);


    private final Report report = new Report();


    /**
     *  Get report of the JMS listener.
     * 
     * @return The report
     */
    public Report getReport() {
        return report;
    }

    /**
     *  Get they way the listener is used.
     * @return Description how listener is used.
     */
    abstract public String getUsageType();


    private Session session;
    private String id = "?";

    /**
     * Set the JMS session so the listener can control the session.
     *
     * @param session The JMS session.
     */
    public void setSession(Session session) {
        this.session = session;
    }

    public Session getSession() {
        return session;
    }

    /**
     * Set human-friendly identifier, handy for debugging.
     *
     * @param id Identifier
     */
    public void setIdentification(String id) {
        this.id = id;
    }

    public String getIdentification() {
        return id;
    }


    @Override
    public void onException(JMSException jmse) {

        getReport().addConnectionError(jmse);

        // Report exception
        StringBuilder sb = new StringBuilder();
        sb.append(jmse.getMessage());

        String txt = jmse.getErrorCode();
        if (txt != null) {
            sb.append(" (").append(txt).append(") ");
        }

        LOG.error(sb.toString(), jmse);

        // If there is a linked exception, report it too
        Exception linkedException = jmse.getLinkedException();
        if (linkedException != null) {
            LOG.error("Linked with: " + linkedException.getMessage(), linkedException);
        }
    }
    
}
