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
import java.util.logging.Level;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import javax.naming.Context;
import javax.naming.InitialContext;
import org.apache.commons.lang3.StringUtils;

import org.apache.log4j.Logger;
import org.exist.dom.QName;
import org.exist.memtree.DocumentImpl;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;

import org.exist.messaging.configuration.JmsConfiguration;
import org.exist.messaging.shared.Constants;
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

    /**
     * States of receiver
     */
    private enum STATE {
        NOT_DEFINED, STARTED, STOPPED, CLOSED
    };
    private STATE state = STATE.NOT_DEFINED;
    /**
     * The JMS listener
     */
    private ReceiverJMSListener myListener = new ReceiverJMSListener();
    /*
     * 
     */
    private FunctionReference ref;
    private JmsConfiguration config;
    private Sequence functionParams;
    private XQueryContext context;
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
     * @param ref Reference to XQUery
     * @param config Configuration parameters JMS
     * @param functionParams option function parameters
     * @param context Xquery context
     */
    public Receiver(FunctionReference ref, JmsConfiguration config, Sequence functionParams, XQueryContext context) {
        this.ref = ref;
        this.config = config;
        this.context = context;
        this.functionParams = functionParams;

        id = UUID.randomUUID().toString();
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

        // JMS specific checks
        config.validate();

        try {
            // Setup listener
            myListener.setFunctionReference(ref);
            myListener.setXQueryContext(context);
            myListener.setFunctionParameters(functionParams);

            // Setup Context
            Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, config.getInitialContextFactory());
            props.setProperty(Context.PROVIDER_URL, config.getProviderURL());
            initialContext = new InitialContext(props);

            // Setup connection
            connectionFactory = (ConnectionFactory) initialContext.lookup(config.getConnectionFactory());

            // Setup username/password when required
            String userName = config.getConnectionUserName();
            String password = config.getConnectionPassword();
            if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
                connection = connectionFactory.createConnection();
            } else {
                connection = connectionFactory.createConnection(userName, password);
            }

            // Setup session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Setup destination
            destination = (Destination) initialContext.lookup(config.getDestination());

            // Setup consumer
            messageConsumer = session.createConsumer(destination);
            messageConsumer.setMessageListener(myListener);


            LOG.info(String.format("JMS connection is initialized. ClientId=%s", connection.getClientID()));

            state = STATE.STOPPED;

        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
            errors.add(t.getMessage());
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
     * @return Get i=details about Receiver and Listener
     */
    public NodeImpl info() {

        MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        // start root element
        int nodeNr = builder.startElement("", "Receiver", "Receiver", null);
        if (id != null) {
            builder.addAttribute(new QName("id", null, null), id);
        }

        builder.startElement("", "NrProcessedMessages", "NrProcessedMessages", null);
        builder.characters("" + myListener.getMessageCounter());
        builder.endElement();

        builder.startElement("", "State", "State", null);
        builder.characters("" + state.name());
        builder.endElement();

        List<String> listenerErrors = myListener.getErrors();
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

        builder.startElement("", Context.INITIAL_CONTEXT_FACTORY, Context.INITIAL_CONTEXT_FACTORY, null);
        builder.characters(config.getInitialContextFactory());
        builder.endElement();

        builder.startElement("", Context.PROVIDER_URL, Context.PROVIDER_URL, null);
        builder.characters(config.getProviderURL());
        builder.endElement();

        builder.startElement("", Constants.CONNECTION_FACTORY, Constants.CONNECTION_FACTORY, null);
        builder.characters(config.getConnectionFactory());
        builder.endElement();

        String userName = config.getConnectionUserName();
        if (StringUtils.isNotBlank(userName)) {
            builder.startElement("", Constants.JMS_CONNECTION_USERNAME, Constants.JMS_CONNECTION_USERNAME, null);
            builder.characters(userName);
            builder.endElement();
        }

        try {
            String clientId = connection.getClientID();
            if (StringUtils.isNotEmpty(clientId)) {
                builder.startElement("", Constants.CLIENT_ID, Constants.CLIENT_ID, null);
                builder.characters(clientId);
                builder.endElement();
            }
        } catch (JMSException ex) {
            //
        }


        builder.startElement("", Constants.DESTINATION, Constants.DESTINATION, null);
        builder.characters(config.getDestination());
        builder.endElement();


        // finish root element
        builder.endElement();

        // return result
        return ((DocumentImpl) builder.getDocument()).getNode(nodeNr);


    }
}
