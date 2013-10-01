/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2013 The eXist Project
 *  http://exist-db.org
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.messaging.shared;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.log4j.Logger;

import org.exist.memtree.DocumentImpl;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;

import static org.exist.messaging.shared.Constants.*;

/**
 *  Helper class ; content might be moved
 * 
 * @author Dannes Wessels
 */
public class Reporter {
    
    private final static Logger LOG = Logger.getLogger(Reporter.class);


    /**
     * Create messaging results report
     *
     * TODO shared code, except context (new copied)
     */
    public static NodeImpl createReport(Message message) {

        MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        // start root element
        int nodeNr = builder.startElement("", JMS, JMS, null);

        try {
            String txt = message.getJMSMessageID();
            if (txt != null) {
                builder.startElement("", JMS_MESSAGE_ID, JMS_MESSAGE_ID, null);
                builder.characters(message.getJMSMessageID());
                builder.endElement();
            }
        } catch (JMSException ex) {
            LOG.error(ex);
        }

        try {
            String txt = message.getJMSCorrelationID();
            if (txt != null) {
                builder.startElement("", JMS_CORRELATION_ID, JMS_CORRELATION_ID, null);
                builder.characters(message.getJMSCorrelationID());
                builder.endElement();
            }
        } catch (JMSException ex) {
            LOG.error(ex);
        }

        try {
            String txt = message.getJMSType();
            if (txt != null) {
                builder.startElement("", JMS_TYPE, JMS_TYPE, null);
                builder.characters(message.getJMSType());
                builder.endElement();
            }
        } catch (JMSException ex) {
            LOG.error(ex);
        }

        // finish root element
        builder.endElement();

        // return result
        return ((DocumentImpl) builder.getDocument()).getNode(nodeNr);


    }
}
