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
package org.exist.jms.send;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import org.exist.dom.NodeProxy;
import org.exist.memtree.DocumentImpl;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;
import org.exist.jms.shared.JmsConfiguration;
import org.exist.jms.shared.JmsMessageProperties;
import org.exist.jms.shared.Constants;

import org.exist.storage.serializers.Serializer;
import org.exist.validation.internal.node.NodeInputStream;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.*;

import static org.exist.jms.shared.Constants.*;
import org.exist.jms.shared.Identity;
import org.exist.jms.shared.eXistMessage;
import org.exist.jms.shared.eXistMessageItem;

/**
 *
 * @author Dannes Wessels
 */
public class Sender  {

    private final static Logger LOG = Logger.getLogger(Sender.class);
    
    private XQueryContext xqcontext;
    private String username;

    public Sender() {
    }

    /**
     * Constructor.
     *
     * @param context Xquery context, can be NULL for eXistMessageItem
     */
    public Sender(XQueryContext context) {
        username = context.getSubject().getName();
        xqcontext = context.copyContext();
    }

    /**
     * Send content to JMS broker.
     *
     * @param jmsConfig JMS configuration
     * @param msgMetaProps JMS message properties
     * @param content The content to be transferred
     * @return Report
     * @throws XPathException Something bad happened.
     */
    public NodeImpl send(JmsConfiguration jmsConfig, JmsMessageProperties msgMetaProps, Item content) throws XPathException {

        // JMS specific checks
        jmsConfig.validate();
        
        // Retrieve and set JMS identifier
        String id  = Identity.getInstance().getIdentity();    
        if(StringUtils.isNotBlank(id)){
            msgMetaProps.setProperty(Constants.EXIST_INSTANCE_ID, id);
        } else {
            LOG.error(String.format("An empty value was provided for '%s'", Constants.EXIST_INSTANCE_ID));
        }

        // Set username
        if (username != null) {
            msgMetaProps.setProperty("exist.user", username);
        }
    

        // Retrieve relevant values
        String initialContextFactory = jmsConfig.getInitialContextFactory();
        String providerURL = jmsConfig.getBrokerURL();  
        String destinationValue = jmsConfig.getDestination();

        Connection connection = null;

        try {
            Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
            props.setProperty(Context.PROVIDER_URL, providerURL);
            javax.naming.Context context = new InitialContext(props);

            // Get connection factory
            ConnectionFactory cf = getConnectionFactoryInstance(context, jmsConfig);

            if (cf == null) {
                throw new XPathException("Unable to create connection factory");
            }
            
            // Setup username/password when required
            String userName = jmsConfig.getConnectionUserName();
            String password = jmsConfig.getConnectionPassword();
            
            connection = (StringUtils.isBlank(userName) || StringUtils.isBlank(password))
                    ? cf.createConnection()
                    : cf.createConnection(userName, password);
            
            // Set clientId when set and not empty
            String clientId=jmsConfig.getClientID();
            if(StringUtils.isNotBlank(clientId)){
                connection.setClientID(clientId);
            }
            
            // Lookup queue
            Destination destination = (Destination) context.lookup(destinationValue);

            // Create session
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            // Create message producer
            MessageProducer producer = session.createProducer(destination);

            // Create message, depending on incoming object type
            boolean isExistMessageItem = (content instanceof eXistMessageItem);
            Message message = isExistMessageItem
                    ? createMessageFromExistMessageItem(session, (eXistMessageItem) content, msgMetaProps)
                    : createMessageFromItem(session, content, msgMetaProps, xqcontext);

            // Set Message properties from user provided data
            setMessagePropertiesFromMap(msgMetaProps, message);

            // Set time-to-live (when available)
            Long timeToLive = jmsConfig.getTimeToLive();
            if (timeToLive != null) {
                producer.setTimeToLive(timeToLive);
            }

            // Set priority (when available)
            Integer priority = jmsConfig.getPriority();
            if (priority != null) {
                producer.setPriority(priority);
            }

            // Send message
            producer.send(message);

            // Return report
            return createReport(message, producer, jmsConfig);

        } catch (Throwable ex) {
            LOG.error(ex);
            throw new XPathException(ex.getMessage());

        } finally {
            try {
                if (connection != null) {
                    // Close connection
                    connection.close();
                }
            } catch (JMSException ex) {
                LOG.error(String.format("Problem closing connection, ignored. %s (%s)", ex.getMessage(), ex.getErrorCode()));
            }
        }
    }

    /**
     * Get connection factory
     */
    private ConnectionFactory getConnectionFactoryInstance(javax.naming.Context context, JmsConfiguration jmsConfig) throws NamingException {

        ConnectionFactory retVal = null;
 
        // check if Pooling is needed
        String poolValue = jmsConfig.getProperty(EXIST_CONNECTION_POOL);
        if (StringUtils.isNotBlank(poolValue)) {

            // Get URL to broker
            String providerURL = jmsConfig.getBrokerURL();

            // Get ConnectionFactory
            retVal = SenderConnectionFactory.getConnectionFactoryInstance(providerURL, poolValue);

        } else {
            // Get name of connection factory
            String connectionFactory = jmsConfig.getConnectionFactory();

            // Get connection factory, the context already contains the brokerURL.
            retVal = (ConnectionFactory) context.lookup(connectionFactory);
        }

        return retVal;
    }

    private static final String EXIST_CONNECTION_POOL = "exist.connection.pool";

    /**
     * Convert messaging-function originated data into a JMS message.
     *
     * @param session The JMS session
     * @param item The XQuery item containing data
     * @param jmp JMS message properties
     * @param xqcontext The XQuery context form the original query
     * @return JMS message
     * @throws JMSException When a problem occurs in the JMS domain
     * @throws XPathException When an other issue occurs
     */
    private Message createMessageFromItem(Session session, Item item, JmsMessageProperties jmp, XQueryContext xqcontext) throws JMSException, XPathException {

        Message message = null;

        jmp.setProperty(EXIST_XPATH_DATATYPE, Type.getTypeName(item.getType()));
        boolean isCompressed = isDataCompressionRequired(jmp);

        if (item.getType() == Type.ELEMENT || item.getType() == Type.DOCUMENT) {
            LOG.debug("Streaming element or document node");
            
            jmp.setProperty(EXIST_DATA_TYPE, DATA_TYPE_XML);

            if (item instanceof NodeProxy) {
                NodeProxy np = (NodeProxy) item;
                jmp.setProperty(EXIST_DOCUMENT_URI, np.getDocument().getBaseURI());
                jmp.setProperty(EXIST_DOCUMENT_MIMETYPE, np.getDocument().getMetadata().getMimeType());
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream os = baos;

            // Stream content node to buffer
            NodeValue node = (NodeValue) item;
            Serializer serializer = xqcontext.getBroker().newSerializer();
            InputStream is = new NodeInputStream(serializer, node);

            try {
                if (isCompressed) {
                    os = new GZIPOutputStream(os);
                }
                IOUtils.copy(is, os);

            } catch (IOException ex) {
                LOG.error(ex);
                throw new XPathException(ex);

            } finally {
                IOUtils.closeQuietly(is);
                IOUtils.closeQuietly(os);
            }

            // Create actual message, pass data
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(baos.toByteArray());

            // Swap
            message = bytesMessage;


        } else if (item.getType() == Type.BASE64_BINARY || item.getType() == Type.HEX_BINARY) {
            
            LOG.debug("Streaming base64 binary");

            jmp.setProperty(EXIST_DATA_TYPE, DATA_TYPE_BINARY);

            if (item instanceof Base64BinaryDocument) {
                Base64BinaryDocument b64doc = (Base64BinaryDocument) item;
                String uri = b64doc.getUrl();

                LOG.debug(String.format("Base64BinaryDocument detected, adding URL %s", uri));
                jmp.setProperty(EXIST_DOCUMENT_URI, uri);
                
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream os = baos;

            // Copy data from item to buffer
            BinaryValue binary = (BinaryValue) item;
            InputStream is = binary.getInputStream();
            try {
                if (isCompressed) {
                    os = new GZIPOutputStream(os);
                }
                IOUtils.copy(is, os);

            } catch (IOException ex) {
                LOG.error(ex);
                throw new XPathException(ex);

            } finally {
                IOUtils.closeQuietly(is);
                IOUtils.closeQuietly(os);
            }

            // Create actual message, pass data
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(baos.toByteArray());

            // Swap
            message = bytesMessage;

        } else if (item.getType() == Type.STRING) {
            // xs:string() is mapped to a TextMessage
            TextMessage textMessage = session.createTextMessage();
            textMessage.setText(item.getStringValue());
            message = textMessage;


        } else {
            ObjectMessage objectMessage = session.createObjectMessage();

            switch (item.getType()) {
                case Type.INTEGER:
                    BigInteger intValue = item.toJavaObject(BigInteger.class);
                    objectMessage.setObject(intValue);
                    break;
                case Type.DOUBLE:
                    Double doubleValue = item.toJavaObject(Double.class);
                    objectMessage.setObject(doubleValue);
                    break;
                case Type.FLOAT:
                    Float foatValue = item.toJavaObject(Float.class);
                    objectMessage.setObject(foatValue);
                    break;
                case Type.DECIMAL:
                    BigDecimal decimalValue = item.toJavaObject(BigDecimal.class);
                    objectMessage.setObject(decimalValue);
                    break;
                case Type.BOOLEAN:
                    Boolean booleanValue = item.toJavaObject(Boolean.class);
                    objectMessage.setObject(booleanValue);
                    break;    
                default:
                    throw new XPathException(
                            String.format("Unable to convert '%s' of type '%s' into a JMS object.", item.getStringValue(), item.getType()));
            }

            // Swap
            message = objectMessage;
        }

        return message;
    }

    /**
     * Convert replication originated data into a JMS message.
      *
     * @param session JMS session
     * @param emi The data
     * @param msgMetaProps Additional JMS message properties
     * @return JMS Message
     * @throws JMSException When an issue happens
     */
    private Message createMessageFromExistMessageItem(Session session, eXistMessageItem emi, JmsMessageProperties msgMetaProps) throws JMSException {

        // Create bytes message
        BytesMessage message = session.createBytesMessage();

        // Set payload when available
        eXistMessage em = emi.getData();

        byte[] payload = em.getPayload();

        if (payload == null) {
            LOG.error("No payload for replication");
        } else {
            message.writeBytes(payload);
        }

        em.updateMessageProperties(message);

        return message;
    }

    /**
     * Determine if the XML/Binary payload needs to be compressed
     *
     * @param mdd The JMS message properties
     * @return TRUE if not set or has value 'gzip' else FALSE.
     *
     */
    private boolean isDataCompressionRequired(JmsMessageProperties mdd) {
        // 
        String compressionValue = mdd.getProperty(EXIST_DOCUMENT_COMPRESSION);
        if (StringUtils.isBlank(compressionValue)) {
            compressionValue = COMPRESSION_TYPE_GZIP;
            mdd.setProperty(EXIST_DOCUMENT_COMPRESSION, COMPRESSION_TYPE_GZIP);
        }

        return COMPRESSION_TYPE_GZIP.equals(compressionValue);
    }

    private void setMessagePropertiesFromMap(JmsMessageProperties msgMetaProps, Message message) throws JMSException {

        if (msgMetaProps == null) {
            LOG.debug("No JmsMessageProperties was provided");
            return;
        }

        // Write message properties
        for (Map.Entry<Object, Object> entry : msgMetaProps.entrySet()) {

            String key = (String) entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String) {
                message.setStringProperty(key, (String) value);

            } else if (value instanceof Integer) {
                message.setIntProperty(key, (Integer) value);

            } else if (value instanceof Double) {
                message.setDoubleProperty(key, (Double) value);

            } else if (value instanceof Boolean) {
                message.setBooleanProperty(key, (Boolean) value);

            } else if (value instanceof Float) {
                message.setFloatProperty(key, (Float) value);

            } else {
                LOG.error(String.format("Cannot set %s into a JMS property", value.getClass().getCanonicalName()));
            }
        }
    }

    /**
     * Create messaging results report
     */
    private NodeImpl createReport(Message message, MessageProducer producer, JmsConfiguration config) {

        MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        // start root element
        int nodeNr = builder.startElement("", JMS, JMS, null);

        /*
         * Message
         */
        if (message != null) {
            try {
                String txt = message.getJMSMessageID();
                if (txt != null) {
                    builder.startElement("", JMS_MESSAGE_ID, JMS_MESSAGE_ID, null);
                    builder.characters(txt);
                    builder.endElement();
                }
            } catch (JMSException ex) {
                LOG.error(ex);
            }

            try {
                String txt = message.getJMSCorrelationID();
                if (txt != null) {
                    builder.startElement("", JMS_CORRELATION_ID, JMS_CORRELATION_ID, null);
                    builder.characters(txt);
                    builder.endElement();
                }
            } catch (JMSException ex) {
                LOG.error(ex);
            }

            try {
                String txt = message.getJMSType();
                if (StringUtils.isNotEmpty(txt)) {
                    builder.startElement("", JMS_TYPE, JMS_TYPE, null);
                    builder.characters(txt);
                    builder.endElement();
                }
            } catch (JMSException ex) {
                LOG.error(ex);
            }
        }

        /*
         * Producer
         */
        if (producer != null) {
            try {
                long timeToLive = producer.getTimeToLive();
                builder.startElement("", PRODUCER_TTL, PRODUCER_TTL, null);
                builder.characters("" + timeToLive);
                builder.endElement();
            } catch (JMSException ex) {
                LOG.error(ex);
            }

            try {
                long priority = producer.getPriority();
                builder.startElement("", PRODUCER_PRIORITY, PRODUCER_PRIORITY, null);
                builder.characters("" + priority);
                builder.endElement();
            } catch (JMSException ex) {
                LOG.error(ex);
            }
        }

        /*
         * Configuration
         */
        if (config != null) {
            builder.startElement("", Context.INITIAL_CONTEXT_FACTORY, Context.INITIAL_CONTEXT_FACTORY, null);
            builder.characters(config.getInitialContextFactory());
            builder.endElement();

            builder.startElement("", Context.PROVIDER_URL, Context.PROVIDER_URL, null);
            builder.characters(config.getBrokerURL());
            builder.endElement();

            builder.startElement("", Constants.CONNECTION_FACTORY, Constants.CONNECTION_FACTORY, null);
            builder.characters(config.getConnectionFactory());
            builder.endElement();

            builder.startElement("", Constants.DESTINATION, Constants.DESTINATION, null);
            builder.characters(config.getDestination());
            builder.endElement();

            String userName = config.getConnectionUserName();
            if (StringUtils.isNotBlank(userName)) {
                builder.startElement("", Constants.JMS_CONNECTION_USERNAME, Constants.JMS_CONNECTION_USERNAME, null);
                builder.characters(userName);
                builder.endElement();
            }
        }


        // finish root element
        builder.endElement();

        // return result
        return ((DocumentImpl) builder.getDocument()).getNode(nodeNr);


    }


}
