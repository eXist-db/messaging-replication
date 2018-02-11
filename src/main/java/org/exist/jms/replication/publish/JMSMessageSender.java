/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2012 The eXist Project
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
 *
 *  $Id$
 */
package org.exist.jms.replication.publish;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.jms.replication.shared.MessageSender;
import org.exist.jms.replication.shared.TransportException;
import org.exist.jms.shared.JmsConfiguration;
import org.exist.jms.shared.JmsMessageProperties;
import org.exist.jms.shared.eXistMessage;
import org.exist.jms.shared.eXistMessageItem;
import org.exist.jms.shared.send.Sender;

import java.util.List;
import java.util.Map;

/**
 * Specific class for sending a eXistMessage via JMS to a broker
 *
 * @author Dannes Wessels
 */
public class JMSMessageSender implements MessageSender {

    private final static Logger LOG = LogManager.getLogger(JMSMessageSender.class);

    private final PublisherParameters parameters = new PublisherParameters();

    /**
     * Constructor
     *
     * @param params Set of (Key,value) parameters for setting JMS routing
     *               instructions, like java.naming.* , destination and connection factory.
     */
    JMSMessageSender(final Map<String, List<?>> params) {
        parameters.setMultiValueParameters(params);
    }

    /**
     * Send {@link eXistMessage} to message broker.
     *
     * @param em The message that needs to be sent
     * @throws TransportException Thrown when something bad happens.
     */
    @Override
    public void sendMessage(final eXistMessage em) throws TransportException {

        try {
            // Get from .xconf file, fill defaults when needed
            parameters.processParameters();

            final Sender sender = new Sender();

            final eXistMessageItem item = new eXistMessageItem();
            item.setData(em);

            final JmsConfiguration jmsConfig = new JmsConfiguration();
            jmsConfig.loadPublisherParameters(parameters);

            final JmsMessageProperties msgMetaProps = new JmsMessageProperties();
            msgMetaProps.loadParameters(parameters);

            sender.send(jmsConfig, msgMetaProps, item);

        } catch (final Throwable ex) {
            // I know, this is bad coding practice,
            // but in case of probles we really need to fire this exception
            LOG.error(ex.getMessage(), ex);
            throw new TransportException(ex.getMessage(), ex);
        }

    }
}
