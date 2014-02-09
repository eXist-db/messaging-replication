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
import javax.jms.MessageListener;
import javax.jms.Session;

/**
 * Interface definition 
 * @author wessels
 */
public interface eXistMessageListener extends MessageListener, ExceptionListener {

    /**
     *  Get report of the JMS listener.
     * 
     * @return The report
     */
    public Report getReport();

    /**
     *  Get they way the listener is used.
     * @return 
     */
    public String getUsageType();
    
    /**
     *  Set the JMS session so the listener can control
     * the session.
     * 
     * @param session The JMS session.
     */
    public void setSession(Session session);
    
    /**
     *  Set human-friendly identifier, handy for debugging.
     * 
     * @param id Identifier
     */
    public void setIdentification(String id);
    
}
