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
package org.exist.messaging.receive;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import org.exist.dom.QName;
import org.exist.memtree.DocumentImpl;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;

import org.exist.messaging.configuration.JmsConfiguration;
import org.exist.messaging.shared.Constants;
import org.exist.messaging.shared.Report;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionReference;
import org.exist.xquery.value.Sequence;

/**
 * JMS messages receiver, represents a JMS connection.
 *
 * Starts a JMS listener to receive messages from the broker
 *
 * @author Dannes Wessels (dannes@exist-db.org)
 */
public class Receiver {

    private final static Logger LOG = Logger.getLogger(Receiver.class);
    private List<String> errors = new ArrayList<String>();
    private DatatypeFactory dtFactory = null;

    /**
     * States of receiver
     */
    private enum STATE {
        NOT_DEFINED, STARTED, STOPPED, CLOSED, ERROR
    };
    private STATE state = STATE.NOT_DEFINED;
    /**
     * The JMS listener
     */
    private MessagingJmsListener myListener = null; //new MessagingJmsListener();
    /*
     * 
     */
    private JmsConfiguration jmsConfig;
    private Context initialContext = null;
    private ConnectionFactory connectionFactory = null;
    private Session session = null;
    private Destination destination = null;
    private MessageConsumer messageConsumer = null;
    private Connection connection = null;
    private String id;

    /**
     * Constructor
     *
     * @param ref Reference to the XQuery function
     * @param config JMSConfiguration parameters
     * @param functionParams Optional function parameters
     * @param context The XQuery context
     */
    public Receiver(JmsConfiguration config, MessagingJmsListener listener) {
        this.jmsConfig = config;
        this.myListener = listener;

        // Uniq ID for Receiver
        id = UUID.randomUUID().toString();

        // Initialing XML datafactory
        try {
            dtFactory = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException ex) {
            LOG.fatal(ex);
        }
    }

    /**
     * Get ID of receiver
     *
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * Start JMS connection
     *
     * @see Connection#start()
     *
     * @throws XPathException Thrown when not initialized or when a JMSException is thrown.
     */
    public void start() throws XPathException {

        if (connection == null) {
            String txt = "JMS connection must be initialized first";
            LOG.error(txt);
            throw new XPathException(txt);
        }

        try {
            // Start listener
            connection.start();

            LOG.info(String.format("JMS connection is started. ClientId=%s", connection.getClientID()));

            state = STATE.STARTED;

        } catch (JMSException ex) {
            LOG.error(ex);
            errors.add(ex.getMessage());
            throw new XPathException(ex.getMessage());
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
            Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, jmsConfig.getInitialContextFactory());
            props.setProperty(Context.PROVIDER_URL, jmsConfig.getProviderURL());
            initialContext = new InitialContext(props);

            // Setup connection
            connectionFactory = (ConnectionFactory) initialContext.lookup(jmsConfig.getConnectionFactory());

            // Setup username/password when required
            String userName = jmsConfig.getConnectionUserName();
            String password = jmsConfig.getConnectionPassword();
            if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
                connection = connectionFactory.createConnection();
            } else {
                connection = connectionFactory.createConnection(userName, password);
            }
            
            // Set clientId when set and not empty
            String clientId=jmsConfig.getClientID();
            if(StringUtils.isNotBlank(clientId)){
                connection.setClientID(clientId);
            }

            // Setup session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Setup destination
            destination = (Destination) initialContext.lookup(jmsConfig.getDestination());

            // Setup consumer with message selector
            String messageSelector = jmsConfig.getMessageSelector();
            String subscriberName = jmsConfig.getSubscriberName();
            
            boolean isDurable = jmsConfig.isDurable(); // TRUE if not set, special case for Durable topic
            boolean isNoLocal = jmsConfig.isNoLocal();
            
            // Interesting switch due to JMS specification
            if (destination instanceof Topic && isDurable) {
                // Create durable subscriber for topic only when set durable manually
                messageConsumer = session.createDurableSubscriber((Topic) destination, subscriberName, messageSelector, isNoLocal);

            } else {
                // When not a Topic OR when a Topic but not durable.....
                messageConsumer = session.createConsumer(destination, messageSelector, isNoLocal);
            }
            
            // Register listeners
            messageConsumer.setMessageListener(myListener);

            LOG.info(String.format("JMS connection is initialized: %s=%s %s=%s %s=%s %s=%s %s=%s",
                    Constants.CLIENT_ID, connection.getClientID(), Constants.MESSAGE_SELECTOR, messageSelector,
                    Constants.SUBSCRIBER_NAME, subscriberName, Constants.DURABLE, isDurable, Constants.NO_LOCAL, isNoLocal));

            state = STATE.STOPPED;

        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
            LOG.debug("" + jmsConfig.toString());
            errors.add(t.getMessage());
            throw new XPathException(t.getMessage());
        }

    }

    /**
     * Stop JMS connection
     *
     * @see Connection#stop()
     *
     * @throws XPathException Thrown when not initialized or when a JMSException is thrown.
     */
    public void stop() throws XPathException {

        if (connection == null) {
            String txt = "JMS connection must be initialized first";
            LOG.error(txt);
            throw new XPathException(txt);
        }

        try {
            // Start listener
            connection.stop();

            LOG.info(String.format("JMS connection is stopped. ClientId=%s", connection.getClientID()));

            state = STATE.STOPPED;

        } catch (JMSException ex) {
            LOG.error(ex);
            errors.add(ex.getMessage());
            throw new XPathException(ex.getMessage());
        }
    }

    /**
     * CLose JMS connection
     *
     * @see Connection#close()
     *
     * @throws XPathException Thrown when not initialized or when a JMSException is thrown.
     */
    public void close() throws XPathException {

        if (connection == null) {
            String txt = "JMS connection must be initialized first";
            LOG.error(txt);
            throw new XPathException(txt);
        }

        // If not stopped, try to stop first
        if (state != STATE.STOPPED) {
            try {
                connection.stop();
            } catch (JMSException ex) {
                LOG.error(ex);
                errors.add(ex.getMessage());
            }
        }

        try {

            String clientId = null;
            try {
                clientId = connection.getClientID();
            } catch (JMSException ex) {
                LOG.debug(ex);
            }

            // Start listener
            connection.close();

            // Report with client ID when available
            if (clientId == null) {
                LOG.info("JMS connection is closed.");
            } else {
                LOG.info(String.format("JMS connection is closed. ClientId=%s", clientId));
            }

            state = STATE.CLOSED;

        } catch (JMSException ex) {
            LOG.error(ex);
            errors.add(ex.getMessage());
            throw new XPathException(ex.getMessage());
        }
    }

    /**
     * @return Get report about Receiver and Listener
     */
    public NodeImpl getReport() {

        MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        // start root element
        int nodeNr = builder.startElement("", "Receiver", "Receiver", null);
        if (id != null) {
            builder.addAttribute(new QName("id", null, null), id);
        }

        /*
         * Internal state
         */
        builder.startElement("", "State", "State", null);
        builder.characters("" + state.name());
        builder.endElement();

        /*
         * JMS configuration
         */
        builder.startElement("", Context.INITIAL_CONTEXT_FACTORY, Context.INITIAL_CONTEXT_FACTORY, null);
        builder.characters(jmsConfig.getInitialContextFactory());
        builder.endElement();

        builder.startElement("", Context.PROVIDER_URL, Context.PROVIDER_URL, null);
        builder.characters(jmsConfig.getProviderURL());
        builder.endElement();

        builder.startElement("", Constants.CONNECTION_FACTORY, Constants.CONNECTION_FACTORY, null);
        builder.characters(jmsConfig.getConnectionFactory());
        builder.endElement();

        String userName = jmsConfig.getConnectionUserName();
        if (StringUtils.isNotBlank(userName)) {
            builder.startElement("", Constants.JMS_CONNECTION_USERNAME, Constants.JMS_CONNECTION_USERNAME, null);
            builder.characters(userName);
            builder.endElement();
        }

        builder.startElement("", Constants.DESTINATION, Constants.DESTINATION, null);
        builder.characters(jmsConfig.getDestination());
        builder.endElement();

        if (connection != null) {
            try {
                String clientId = connection.getClientID();
                if (StringUtils.isNotEmpty(clientId)) {
                    builder.startElement("", Constants.CLIENT_ID, Constants.CLIENT_ID, null);
                    builder.characters(clientId);
                    builder.endElement();
                }
            } catch (JMSException ex) {
                LOG.debug(ex.getMessage());
            }
        }

        /*
         * Message consumer
         */
        if (messageConsumer != null) {
            try {
                String messageSelector = messageConsumer.getMessageSelector();
                if (messageSelector != null) {
                    builder.startElement("", Constants.MESSAGE_SELECTOR, Constants.MESSAGE_SELECTOR, null);
                    builder.characters(messageSelector);
                    builder.endElement();
                }
            } catch (JMSException ex) {
                LOG.debug(ex.getMessage());
            }
        }


        /*
         * Statistics & error reporting 
         */
        if (myListener != null) {
            /*
             * Error reproting
             */
            List<String> listenerErrors = myListener.getReport().getErrors();
            if (!listenerErrors.isEmpty() || !errors.isEmpty()) {
                builder.startElement("", "ErrorMessages", "ErrorMessages", null);

                if (!listenerErrors.isEmpty()) {
                    for (String error : listenerErrors) {
                        builder.startElement("", "Error", "Error", null);
                        builder.addAttribute(new QName("src", null, null), "listener");
                        builder.characters(error);
                        builder.endElement();
                    }
                }

                if (!errors.isEmpty()) {
                    for (String error : errors) {
                        builder.startElement("", "Error", "Error", null);
                        builder.addAttribute(new QName("src", null, null), "receiver");
                        builder.characters(error);
                        builder.endElement();
                    }

                }

                builder.endElement();
            }

            /*
             * Statistics
             */
            Report stats = myListener.getReport();
            builder.startElement("", "Statistics", "Statistics", null);

            builder.startElement("", "NrProcessedMessages", "NrProcessedMessages", null);
            builder.characters("" + stats.getMessageCounterTotal());
            builder.endElement();

            builder.startElement("", "CumulativeProcessingTime", "CumulativeProcessingTime", null);
            Duration duration = dtFactory.newDuration(stats.getCumulatedProcessingTime());
            builder.characters(duration.toString());
            builder.endElement();

            builder.startElement("", "NrUnprocessedMessages", "NrUnprocessedMessages", null);
            builder.characters("" + stats.getMessageCounterNOK());
            builder.endElement();

            builder.endElement();
        }

        // finish root element
        builder.endElement();

        // return result
        return ((DocumentImpl) builder.getDocument()).getNode(nodeNr);
    }
}
