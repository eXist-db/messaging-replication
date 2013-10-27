package org.exist.messaging.misc;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

/**
 *  Simple JMS data logger. Writes all incoming JMS data to the logger.
 * 
 * @author Dannes Wessels
 */
public class MessageLogger {

    private final static Logger LOG = Logger.getLogger(MessageLogger.class);

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        // Configure logger
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();

        final String testDestination = "dynamicQueues/eXistdbTest";

        try {
            // Setup listener
            MyJMSListener myListener = new MyJMSListener();
            
            // Setup Context
            Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
            props.setProperty(Context.PROVIDER_URL, "tcp://myserver.local:61616");
            Context context = new InitialContext(props);

            // Setup connection
            ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("ConnectionFactory");
            Connection connection = connectionFactory.createConnection();
            connection.setExceptionListener(new MyExceptionListener());
            
            // Setup session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Setup destination
            Destination destination = (Destination) context.lookup(testDestination);
            LOG.info("Destination=" + destination);

            // Setup consumer
            MessageConsumer messageConsumer = session.createConsumer(destination);
            messageConsumer.setMessageListener(myListener);

            // Start listener
            connection.start();

            LOG.info("Receiver is ready");

        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
        }
    }


}
