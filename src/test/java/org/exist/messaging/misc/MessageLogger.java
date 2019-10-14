package org.exist.messaging.misc;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

/**
 * Simple JMS data logger. Writes all incoming JMS data to the logger.
 *
 * @author Dannes Wessels
 */
public class MessageLogger {

    private final static Logger LOG = LogManager.getLogger();

    /**
     * @param args the command line arguments
     */
    public static void main(final String[] args) {

        // Configure logger
//        BasicConfigurator.resetConfiguration();
//        BasicConfigurator.configure();

        final String testDestination = "dynamicQueues/eXistdbTest";

        try {
            // Setup listener
            final MyJMSListener myListener = new MyJMSListener();

            // Setup Context
            final Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
            props.setProperty(Context.PROVIDER_URL, "tcp://localhost:61616");
            final Context context = new InitialContext(props);

            // Setup connection
            final ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("ConnectionFactory");
            final Connection connection = connectionFactory.createConnection();
            connection.setExceptionListener(new MyExceptionListener());

            // Setup session
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Setup destination
            final Destination destination = (Destination) context.lookup(testDestination);
            LOG.info("Destination={}", destination);

            // Setup consumer
            final MessageConsumer messageConsumer = session.createConsumer(destination);
            messageConsumer.setMessageListener(myListener);

            // Start listener
            connection.start();

            LOG.info("Receiver is ready");

        } catch (final Throwable t) {
            LOG.error(t.getMessage(), t);
        }
    }


}
