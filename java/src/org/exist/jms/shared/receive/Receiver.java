/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2017 The eXist Project
 * http://exist-db.org
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.jms.shared.receive;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.dom.QName;
import org.exist.dom.memtree.MemTreeBuilder;
import org.exist.dom.memtree.NodeImpl;
import org.exist.jms.shared.Constants;
import org.exist.jms.shared.JmsConfiguration;
import org.exist.jms.shared.Report;
import org.exist.jms.shared.eXistMessagingListener;
import org.exist.xquery.XPathException;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import java.util.Properties;

import static org.exist.jms.shared.ErrorCodes.*;

/**
 * JMS messages receiver, represents a JMS connection.
 * <p>
 * Starts a JMS listener to receive messages from the broker
 *
 * @author Dannes Wessels (dannes@exist-db.org)
 */
public class Receiver {

    private final static Logger LOG = LogManager.getLogger(Receiver.class);
    private static volatile int lastId = 0;
    /*
     *
     */
    private final JmsConfiguration jmsConfig;
    private DatatypeFactory dtFactory = null;
    private STATE state = STATE.NOT_DEFINED;
    /**
     * The JMS listeners
     */
    private eXistMessagingListener messageListener = null;
    private Context initialContext = null;
    private ConnectionFactory connectionFactory = null;
    private Session session = null;
    private Destination destination = null;
    private MessageConsumer messageConsumer = null;
    private Connection connection = null;

    private int id = 0;

    /**
     * Constructor
     *
     * @param config   JMS configuration settings
     * @param listener The generic exist-db message listener
     */
    public Receiver(final JmsConfiguration config, final eXistMessagingListener listener) {
        this.jmsConfig = config;
        this.messageListener = listener;

        // Uniq ID for Receiver
        id = getIncrementedID();

        listener.setReceiverID(id);

        // Initialing XML datafactory
        try {
            dtFactory = DatatypeFactory.newInstance();
        } catch (final DatatypeConfigurationException ex) {
            LOG.fatal(ex);
        }
    }

    private static synchronized Integer getIncrementedID() {
        lastId++;
        return lastId;
    }

    /**
     * Get ID of receiver
     *
     * @return ID of receiver
     */
    public Integer getReceiverId() {
        return id;
    }

    /**
     * Get report of message listener
     *
     * @return Report
     */
    public Report getReport() {
        return messageListener.getReport();
    }

    /**
     * Start JMS connection
     *
     * @throws XPathException Thrown when not initialized or when a JMSException is thrown.
     * @see Connection#start()
     */
    public void start() throws XPathException {

        if (connection == null) {
            final String txt = "JMS connection must be initialized first";
            LOG.error(txt);
            throw new XPathException(JMS025, txt);
        }

        try {
            // Start listener
            connection.start();

            LOG.info("JMS connection is started. ClientId={}", connection.getClientID());

            state = STATE.STARTED;

        } catch (final JMSException ex) {
            LOG.error(ex.getMessage(), ex);
            messageListener.getReport().addReceiverError(ex);
            throw new XPathException(JMS004, ex.getMessage());
        }
    }

    /**
     * Initialize JMS connection
     *
     * @throws XPathException Thrown when not initialized or when a JMSException is thrown.
     */
    public void initialize() throws XPathException {

        LOG.info("Initializing JMS connection");

        // JMS specific checks
        jmsConfig.validate();

        try {
            // Setup Context
            final Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, jmsConfig.getInitialContextFactory());
            props.setProperty(Context.PROVIDER_URL, jmsConfig.getBrokerURL());
            initialContext = new InitialContext(props);

            // Setup connection
            connectionFactory = (ConnectionFactory) initialContext.lookup(jmsConfig.getConnectionFactory());

            // Setup username/password when required
            final String userName = jmsConfig.getConnectionUserName();
            final String password = jmsConfig.getConnectionPassword();
            if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
                connection = connectionFactory.createConnection();
            } else {
                connection = connectionFactory.createConnection(userName, password);
            }

            // Register error listener
            connection.setExceptionListener(messageListener);

            // Set clientId when set and not empty
            final String clientId = jmsConfig.getClientId();
            if (StringUtils.isNotBlank(clientId)) {
                connection.setClientID(clientId);
            }

            // Setup session
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            // Setup destination
            destination = (Destination) initialContext.lookup(jmsConfig.getDestination());

            // Setup consumer with message selector
            final String messageSelector = jmsConfig.getMessageSelector();
            final String subscriberName = jmsConfig.getSubscriberName();

            final boolean isDurable = jmsConfig.isDurable(); // TRUE if not set, special case for Durable topic
            final boolean isNoLocal = jmsConfig.isNoLocal();

            // Interesting switch due to JMS specification
            if (destination instanceof Topic && isDurable) {
                // Create durable subscriber for topic only when set durable manually
                messageConsumer = session.createDurableSubscriber((Topic) destination, subscriberName, messageSelector, isNoLocal);

                LOG.info("Created durable subscriber for {}", jmsConfig.getDestination());

            } else {
                // When not a Topic OR when a Topic but not durable.....
                messageConsumer = session.createConsumer(destination, messageSelector, isNoLocal);

                LOG.info("Created non-durable subscriber for {}", jmsConfig.getDestination());
            }

            // Register listener
            messageListener.setSession(session);
            messageConsumer.setMessageListener(messageListener);

            if (LOG.isDebugEnabled()) {
                LOG.debug("JMS connection is initialized: {}={} {}", Constants.CLIENT_ID, connection.getClientID(), jmsConfig.toString());
            } else {
                LOG.info("JMS connection is initialized: {}={}", Constants.CLIENT_ID, connection.getClientID());
            }


            state = STATE.STOPPED;

        } catch (final Throwable t) {
            state = STATE.ERROR;

            closeAllSilently(initialContext, connection, session);

            LOG.error(t.getMessage(), t);
            LOG.debug("{}", jmsConfig.toString());

            messageListener.getReport().addReceiverError(t);
            throw new XPathException(JMS000, t.getMessage());
        }

    }

    /**
     * Stop JMS connection
     *
     * @throws XPathException Thrown when not initialized or when a JMSException is thrown.
     * @see Connection#stop()
     */
    public void stop() throws XPathException {

        if (connection == null) {
            final String txt = "JMS connection must be initialized first";
            LOG.error(txt);
            throw new XPathException(JMS025, txt);
        }

        try {
            // Start listener
            connection.stop();

            LOG.info("JMS connection is stopped. ClientId={}", connection.getClientID());

            state = STATE.STOPPED;

        } catch (final JMSException ex) {
            LOG.error(ex.getMessage(), ex);

            messageListener.getReport().addReceiverError(ex);
            throw new XPathException(JMS004, ex.getMessage());
        }
    }

    /**
     * CLose JMS connection
     *
     * @throws XPathException Thrown when not initialized or when a JMSException is thrown.
     * @see Connection#close()
     */
    public void close() throws XPathException {

        if (connection == null) {
            final String txt = "JMS connection must be initialized first";
            LOG.error(txt);
            throw new XPathException(JMS025, txt);
        }

        // If not stopped, try to stop first
        if (state != STATE.STOPPED) {
            try {
                connection.stop();
            } catch (final JMSException ex) {
                LOG.error(ex);
                messageListener.getReport().addReceiverError(ex);
            }
        }

        try {

            String clientId = null;
            try {
                clientId = connection.getClientID();
            } catch (final JMSException ex) {
                LOG.debug(ex.getMessage(), ex);
            }

            // Start listener
            connection.close();

            // Report with client ID when available
            if (clientId == null) {
                LOG.info("JMS connection is closed.");
            } else {
                LOG.info("JMS connection is closed. ClientId={}", clientId);
            }

            state = STATE.CLOSED;

        } catch (final JMSException ex) {
            LOG.error(ex.getMessage(), ex);

            messageListener.getReport().addReceiverError(ex);
            throw new XPathException(JMS004, ex.getMessage());
        }
    }

    /**
     * @return Get report about Receiver and Listener
     */
    public NodeImpl generateReport() {

        final MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        // start root element
        final int nodeNr = builder.startElement("", "Receiver", "Receiver", null);
        builder.addAttribute(new QName("id", null, null), "" + id);

        // Internal state
        builder.startElement("", "state", "state", null);
        builder.characters("" + state.name());
        builder.endElement();

        // JMS configuration
        builder.startElement("", Context.INITIAL_CONTEXT_FACTORY, Context.INITIAL_CONTEXT_FACTORY, null);
        builder.characters(jmsConfig.getInitialContextFactory());
        builder.endElement();

        builder.startElement("", Context.PROVIDER_URL, Context.PROVIDER_URL, null);
        builder.characters(jmsConfig.getBrokerURL());
        builder.endElement();

        builder.startElement("", Constants.CONNECTION_FACTORY, Constants.CONNECTION_FACTORY, null);
        builder.characters(jmsConfig.getConnectionFactory());
        builder.endElement();

        // Username
        final String userName = jmsConfig.getConnectionUserName();
        if (StringUtils.isNotBlank(userName)) {
            builder.startElement("", Constants.JMS_CONNECTION_USERNAME, Constants.JMS_CONNECTION_USERNAME, null);
            builder.characters(userName);
            builder.endElement();
        }

        // Destination
        builder.startElement("", Constants.DESTINATION, Constants.DESTINATION, null);
        builder.characters(jmsConfig.getDestination());
        builder.endElement();

        // Connection
        if (connection != null) {
            try {
                final String clientId = connection.getClientID();
                if (StringUtils.isNotEmpty(clientId)) {
                    builder.startElement("", Constants.CLIENT_ID, Constants.CLIENT_ID, null);
                    builder.characters(clientId);
                    builder.endElement();
                }
            } catch (final JMSException ex) {
                LOG.debug(ex.getMessage());
            }
        }

        // Message consumer
        if (messageConsumer != null) {
            try {
                final String messageSelector = messageConsumer.getMessageSelector();
                if (messageSelector != null) {
                    builder.startElement("", Constants.MESSAGE_SELECTOR, Constants.MESSAGE_SELECTOR, null);
                    builder.characters(messageSelector);
                    builder.endElement();
                }
            } catch (final JMSException ex) {
                LOG.debug(ex.getMessage());
            }
        }


        // Statistics & error reporting
        if (messageListener != null) {
            // Context of usage
            builder.startElement("", "usage", "usage", null);
            builder.characters("" + messageListener.getUsageType());
            builder.endElement();

            // Error reporting
            messageListener.getReport().write(builder);

            // Statistics
            final Report stats = messageListener.getReport();
            builder.startElement("", "statistics", "statistics", null);

            builder.startElement("", "nrProcessedMessages", "nrProcessedMessages", null);
            builder.characters("" + stats.getMessageCounterTotal());
            builder.endElement();

            builder.startElement("", "cumulativeProcessingTime", "cumulativeProcessingTime", null);
            final Duration duration = dtFactory.newDuration(stats.getCumulatedProcessingTime());
            builder.characters(duration.toString());
            builder.endElement();

            builder.startElement("", "nrFailedMessages", "nrFailedMessages", null);
            builder.characters("" + stats.getMessageCounterNOK());
            builder.endElement();

            builder.endElement();
        }

        // finish root element
        builder.endElement();

        // return result
        return builder.getDocument().getNode(nodeNr);
    }

    /**
     * Helper method to give resources back
     */
    private void closeAllSilently(final Context context, final Connection connection, final Session session) {

        final boolean doLog = LOG.isDebugEnabled();

        if (session != null) {
            if (doLog) {
                LOG.debug("Closing session");
            }

            try {
                session.close();
            } catch (final JMSException ex) {
                LOG.error(ex.getMessage());
            }
        }

        if (connection != null) {
            if (doLog) {
                LOG.debug("Closing connection");
            }

            try {
                connection.close();
            } catch (final JMSException ex) {
                LOG.error(ex.getMessage());
            }
        }

        if (context != null) {
            if (doLog) {
                LOG.debug("Closing context");
            }

            try {
                context.close();
            } catch (final NamingException ex) {
                LOG.error(ex.getMessage());
            }
        }
    }

    /**
     * States of receiver
     */
    private enum STATE {
        NOT_DEFINED, STARTED, STOPPED, CLOSED, ERROR
    }
}
