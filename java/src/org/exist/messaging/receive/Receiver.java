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
import java.util.logging.Level;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.log4j.Logger;
import org.exist.dom.QName;
import org.exist.memtree.DocumentImpl;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;

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
    
    private List<String> errors = new ArrayList<String>();
    
    private enum STATE { READY,  INITIALIZED, STARTED, STOPPED, CLOSED};
    
    private STATE state = STATE.READY;
    
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
            
            state = STATE.INITIALIZED;

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

        try {
            // Start listener
            connection.close();

            LOG.info(String.format("JMS connection is closed. ClientId=%s", connection.getClientID()));
            
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
     public  NodeImpl info(String id) {

        MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        // start root element
        int nodeNr = builder.startElement("", "Receiver", "Receiver", null);
        if(id!=null){
         builder.addAttribute(new QName("id", null, null), id);
        }
        
         builder.startElement("", "NrProcessedMessages", "NrProcessedMessages", null);
         builder.characters("" + myListener.getMessageCounter());
         builder.endElement();
        
         builder.startElement("", "State", "State", null);
         builder.characters("" + state.name() );
         builder.endElement();
         
         List<String> listenerErrors = myListener.getErrors();
         if (!listenerErrors.isEmpty() || !errors.isEmpty()) {
             builder.startElement("", "ErrorMessages", "ErrorMessages", null);

             if (!listenerErrors.isEmpty()) {
                 for (String error : listenerErrors) {
                     builder.startElement("", "ListenerError", "ListenerError", null);
                     builder.characters(error);
                     builder.endElement();
                 }    
             }
             
             if (!errors.isEmpty()) {
                 for (String error : errors) {
                     builder.startElement("", "ReceiverError", "ReceiverError", null);
                     builder.characters(error);
                     builder.endElement();
                 }
                 
             }
             
             builder.endElement();
         }
         
         builder.startElement("", Context.INITIAL_CONTEXT_FACTORY, Context.INITIAL_CONTEXT_FACTORY, null);
         builder.addAttribute(QName.TEXT_QNAME, null);
         builder.characters(config.getInitialContextFactory());
         builder.endElement();
         
         builder.startElement("", Context.PROVIDER_URL, Context.PROVIDER_URL, null);
         builder.characters(config.getProviderURL());
         builder.endElement();
         
         builder.startElement("", "ConnectionFactory", "ConnectionFactory", null);
         builder.characters(config.getConnectionFactory());
         builder.endElement();

         try {
             String clientId = connection.getClientID();
             if (clientId != null) {
                 builder.startElement("", "ClientID", "ClientID", null);
                 builder.characters(clientId);
                 builder.endElement();
             }
         } catch (JMSException ex) {
             //
         }
         
         
         builder.startElement("", "Destination", "Destination", null);
         builder.characters(config.getDestination());
         builder.endElement();
         
         
        // finish root element
        builder.endElement();

        // return result
        return ((DocumentImpl) builder.getDocument()).getNode(nodeNr);


    }
}
