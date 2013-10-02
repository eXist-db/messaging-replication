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

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.log4j.Logger;

import org.exist.messaging.configuration.JmsConfiguration;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionReference;

/**
 * JMS messages receiver, represents a JMS connection.
 *
 * Starts a JMS listener to receive messages from the broker
 *
 * @author Dannes Wessels (dannes@exist-db.org)
 */
public class Receiver {

    private final static Logger LOG = Logger.getLogger(Receiver.class);
    private ReceiverJMSListener myListener = new ReceiverJMSListener();
    private FunctionReference ref;
    private JmsConfiguration config;
    private XQueryContext context;
    private Context initialContext = null;
    private ConnectionFactory connectionFactory = null;
    private Session session = null;
    private Destination destination = null;
    private MessageConsumer messageConsumer = null;
    private Connection connection = null;

    /**
     * Constructor
     *
     * @param ref Reference to XQUery
     * @param config Configuration parameters JMS
     * @param context Xquery context
     */
    public Receiver(FunctionReference ref, JmsConfiguration config, XQueryContext context) {
        this.ref = ref;
        this.config = config;
        this.context = context;
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

        } catch (JMSException ex) {
            LOG.error(ex);
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

            // Setup Context
            Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, config.getInitialContextFactory());
            props.setProperty(Context.PROVIDER_URL, config.getProviderURL());
            initialContext = new InitialContext(props);

            // Setup connection
            connectionFactory = (ConnectionFactory) initialContext.lookup(config.getConnectionFactory());
            connection = connectionFactory.createConnection();

            // Setup session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Setup destination
            destination = (Destination) initialContext.lookup(config.getDestination());

            // Setup consumer
            messageConsumer = session.createConsumer(destination);
            messageConsumer.setMessageListener(myListener);


            LOG.info(String.format("JMS connection is initialized. ClientId=%s", connection.getClientID()));

        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
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

        } catch (JMSException ex) {
            LOG.error(ex);
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

        try {
            // Start listener
            connection.close();

            LOG.info(String.format("JMS connection is closed. ClientId=%s", connection.getClientID()));

        } catch (JMSException ex) {
            LOG.error(ex);
            throw new XPathException(ex.getMessage());
        }
    }
}
