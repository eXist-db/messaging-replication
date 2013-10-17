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
package org.exist.replication.jms.subscribe;

import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.exist.messaging.configuration.JmsConfiguration;
import org.exist.messaging.receive.Receiver;
import org.exist.messaging.receive.ReceiversManager;
import org.exist.storage.DBBroker;
import org.exist.storage.StartupTrigger;

/**
 * Startup Trigger to fire-up a message receiver. Typically this trigger is started by
 * configuration in conf.xml
 *
 * @author Dannes Wessels
 */
public class MessageReceiverStartupTrigger implements StartupTrigger {

    private final static Logger LOG = Logger.getLogger(MessageReceiverStartupTrigger.class);
   

    /*
     * Entry point for starting the trigger.
     */
    @Override
    public void execute(final DBBroker broker, final Map<String, List<? extends Object>> params) {

        ReceiversManager manager = ReceiversManager.getInstance();
        
        // Get from .xconf file, fill defaults when needed
        SubscriberParameters parameters = new SubscriberParameters();
        parameters.setSingleValueParameters(params);
        
//        Context context = null;
//        Connection connection = null;
//        Session session = null;
        try {
            // Get parameters, fill defaults when needed
            parameters.processParameters();

            JmsConfiguration jmsConfig = new JmsConfiguration();
            jmsConfig.loadSubscriberParameters(parameters);

            // Setup listeners
            ReplicationJmsListener jmsListener = new ReplicationJmsListener(broker.getBrokerPool());

            Receiver receiver = new Receiver(jmsConfig, jmsListener);
            manager.register(receiver);

            receiver.initialize();
            receiver.start();


            //            final String subscriberName = parameters.getSubscriberName();
//            final String destination = parameters.getDestination();
//            final String connectionFactory = parameters.getConnectionFactory();
//            final String clientId = parameters.getClientId();
//            final boolean durable = parameters.isDurable();
//            final String messageSelector = parameters.getMessageSelector();
//            final boolean noLocal = parameters.isNoLocal();


//            LOG.info(String.format("Starting subscription of '%s' to '%s'", subscriberName, destination));
//
//            if(LOG.isDebugEnabled()){
//                LOG.debug(parameters.getReport());
//            }

//            // Setup listeners
//            ReplicationJmsListener jmsListener = new ReplicationJmsListener(broker.getBrokerPool());
//            ExceptionListener exceptionListener = new JmsConnectionExceptionListener();
//        
//            // Setup context
//            Properties contextProps = parameters.getInitialContextProps();
//            context = new InitialContext(contextProps);
//
//            // Lookup topic
//            Destination dest = (Destination) context.lookup(destination);
//            if (!(dest instanceof Topic)) {
//                String errorText = String.format("'%s' is not a Topic.", destination);
//                LOG.error(errorText);
//                throw new Exception(errorText); //TODO better exception?
//            }
//
//
//            // Lookup connection factory            
//            ConnectionFactory cf = (ConnectionFactory) context.lookup(connectionFactory);
//            
//            // Set specific properties on the connection factory
//            JmsConnectionHelper.configureConnectionFactory(cf, parameters);
//
//            // Setup connection
//            connection = cf.createConnection();
//            
//            // Register for exceptions
//            connection.setExceptionListener(exceptionListener);
//
//
//            // Set clientId
//            connection.setClientID(clientId);
//
//            // TODO DW: should this be configurable?
//            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//
//            // Set durable messaging, when required
//            if (durable) {
//                // Set subscriber
//                TopicSubscriber topicSubscriber = session.createDurableSubscriber(
//                        (Topic) dest, subscriberName, messageSelector, noLocal);
//
//                // Register listeners
//                topicSubscriber.setMessageListener(jmsListener);
//
//            } else {
//                // Create message consumer
//                MessageConsumer messageConsumer = session.createConsumer(
//                        dest,
//                        messageSelector,
//                        noLocal);
//
//
//                // Register listeners
//                messageConsumer.setMessageListener(jmsListener);
//            }
//
//            // Start it all
//            connection.start();

            LOG.info("Subscription was successful.");

        } catch (final Throwable t) {
//            // Close all that has been opened. Always.
//            closeAll(context, connection, session);
            
            LOG.error(String.format("Unable to start subscription: %s", t.getMessage()), t);
        }
    }

//    /**
//     * Helper method to give resources back
//     */
//    private void closeAll(Context context, Connection connection, Session session) {
//
//        boolean doLog = LOG.isDebugEnabled();
//
//        if (session != null) {
//            if (doLog) {
//                LOG.debug("Closing session");
//            }
//
//            try {
//                session.close();
//            } catch (JMSException ex) {
//                LOG.error(ex.getMessage());
//            }
//        }
//
//        if (connection != null) {
//            if (doLog) {
//                LOG.debug("Closing connection");
//            }
//
//            try {
//                connection.close();
//            } catch (JMSException ex) {
//                LOG.error(ex.getMessage());
//            }
//        }
//
//        if (context != null) {
//            if (doLog) {
//                LOG.debug("Closing context");
//            }
//
//            try {
//                context.close();
//            } catch (NamingException ex) {
//                LOG.error(ex.getMessage());
//            }
//        }
//    }
}
