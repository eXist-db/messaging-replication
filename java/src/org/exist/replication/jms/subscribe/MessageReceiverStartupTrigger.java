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
import java.util.Properties;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.log4j.Logger;
import org.exist.replication.shared.JmsConnectionExceptionListener;
import org.exist.replication.shared.JmsConnectionHelper;
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
        
        // Get from .xconf file, fill defaults when needed
        SubscriberParameters parameters = new SubscriberParameters();
        parameters.setSingleValueParameters(params);
        
        Context context = null;
        Connection connection = null;
        Session session = null;
        try {
            // Get parameters, fill defaults when needed
            parameters.processParameters();

            LOG.info(String.format("Starting subscription of '%s' to '%s'", parameters.getSubscriberName(), parameters.getTopic()));

            if(LOG.isDebugEnabled()){
                LOG.debug(parameters.getReport());
            }

            // Setup listeners
            ReplicationJmsListener jmsListener = new ReplicationJmsListener(broker.getBrokerPool());
            ExceptionListener exceptionListener = new JmsConnectionExceptionListener();
        
            // Setup context
            Properties contextProps = parameters.getInitialContextProps();
            context = new InitialContext(contextProps);

            // Lookup topic
            Destination destination = (Destination) context.lookup(parameters.getTopic());
            if (!(destination instanceof Topic)) {
                String errorText = String.format("'%s' is not a Topic.", parameters.getTopic());
                LOG.error(errorText);
                throw new Exception(errorText); //TODO better exception?
            }

            // Lookup connection factory            
            ConnectionFactory cf = (ConnectionFactory) context.lookup(parameters.getConnectionFactory());
            
            // Set specific properties on the connection factory
            JmsConnectionHelper.configureConnectionFactory(cf, parameters);

            // Setup connection
            connection = cf.createConnection();
            
            // Register for exceptions
            connection.setExceptionListener(exceptionListener);

            // Set clientId
            connection.setClientID(parameters.getClientId());

            // TODO DW: should this be configurable?
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Set durable messaging, when required
            if (parameters.isDurable()) {
                // Set subscriber
                TopicSubscriber topicSubscriber = session.createDurableSubscriber(
                        (Topic) destination,
                        parameters.getSubscriberName(),
                        parameters.getMessageSelector(),
                        parameters.isNoLocal());

                // Register listeners
                topicSubscriber.setMessageListener(jmsListener);

            } else {
                // Create message consumer
                MessageConsumer messageConsumer = session.createConsumer(
                        destination,
                        parameters.getMessageSelector(),
                        parameters.isNoLocal());


                // Register listeners
                messageConsumer.setMessageListener(jmsListener);
            }

            // Start it all
            connection.start();

            LOG.info("Subscription was successful.");

        } catch (final Throwable t) {
            // Close all that has been opened. Always.
            closeAll(context, connection, session);
            
            LOG.error(String.format("Unable to start subscription: %s;  %s", t.getMessage(), parameters.getReport()), t);
        }
    }

    /**
     * Helper method to give resources back
     */
    private void closeAll(Context context, Connection connection, Session session) {

        boolean doLog = LOG.isDebugEnabled();

        if (session != null) {
            if (doLog) {
                LOG.debug("Closing session");
            }

            try {
                session.close();
            } catch (JMSException ex) {
                LOG.error(ex);
            }
        }

        if (connection != null) {
            if (doLog) {
                LOG.debug("Closing connection");
            }

            try {
                connection.close();
            } catch (JMSException ex) {
                LOG.error(ex);
            }
        }

        if (context != null) {
            if (doLog) {
                LOG.debug("Closing context");
            }

            try {
                context.close();
            } catch (NamingException ex) {
                LOG.error(ex);
            }
        }
    }
}
