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
package org.exist.messaging.xquery;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Enumeration;
import java.util.Properties;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;

import org.exist.Namespaces;
import org.exist.memtree.DocumentImpl;
import org.exist.memtree.SAXAdapter;
import org.exist.messaging.configuration.JmsConfiguration;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.validation.ValidationReport;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.Base64BinaryDocument;
import org.exist.xquery.value.BinaryValue;
import org.exist.xquery.value.BooleanValue;
import org.exist.xquery.value.DecimalValue;
import org.exist.xquery.value.DoubleValue;
import org.exist.xquery.value.FloatValue;
import org.exist.xquery.value.FunctionReference;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.ValueSequence;

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * JMS messages receiver.
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

    Receiver(FunctionReference ref, JmsConfiguration config, XQueryContext context) {
        this.ref = ref;
        this.config = config;
        this.context = context;
    }

    /**
     * Start listener
     */
    void start() {

        try {
            // Setup listener
            myListener.setFunctionReference(ref);
            myListener.setXQueryContext(context);

            // Setup Context
            Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, config.getInitialContextFactory());
            props.setProperty(Context.PROVIDER_URL, config.getProviderURL());
            Context initialContext = new InitialContext(props);

            // Setup connection
            ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup(config.getConnectionFactory());
            Connection connection = connectionFactory.createConnection();

            // Setup session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Setup destination
            Destination destination = (Destination) initialContext.lookup(config.getDestination());

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
