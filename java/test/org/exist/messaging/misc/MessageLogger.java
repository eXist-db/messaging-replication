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
 *
 * @author wessels
 */
public class MessageLogger {

    private final static Logger LOG = Logger.getLogger(MessageLogger.class);

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();

        try {
            Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
            props.setProperty(Context.PROVIDER_URL, "tcp://myserver.local:61616");
            Context context = new InitialContext(props);

            ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("ConnectionFactory");

            MyJMSListener myListener = new MyJMSListener();

            Destination destination = (Destination) context.lookup("dynamicTopics/eXistdbTest");

            LOG.info("Destination=" + destination);

            Connection connection = connectionFactory.createConnection();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer messageConsumer = session.createConsumer(destination);

            messageConsumer.setMessageListener(myListener);

            connection.start();


            LOG.info("Receiver is ready");

        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
        }
    }


}
