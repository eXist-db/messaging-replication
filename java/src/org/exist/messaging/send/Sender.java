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
 */
package org.exist.messaging.send;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Properties;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import org.exist.dom.NodeProxy;
import org.exist.memtree.NodeImpl;
import org.exist.messaging.configuration.JmsConfiguration;
import org.exist.messaging.configuration.JmsMessageProperties;
import org.exist.messaging.shared.Reporter;
import org.exist.storage.serializers.Serializer;
import org.exist.validation.internal.node.NodeInputStream;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.*;

import static org.exist.messaging.shared.Constants.*;

/**
 *
 * @author Dannes Wessels
 */
public class Sender  {

    private final static Logger LOG = Logger.getLogger(Sender.class);
    
    private XQueryContext xqcontext;

    public Sender(XQueryContext context) {
        xqcontext = context.copyContext();
    }


    public NodeImpl send(JmsConfiguration jmsConfig, JmsMessageProperties msgMetaProps, Item content) throws XPathException {

        // JMS specific checks
        jmsConfig.validate();

        // Retrieve relevant values
        String initialContextFactory = jmsConfig.getInitialContextFactory();
        String providerURL = jmsConfig.getProviderURL();
        String connectionFactory = jmsConfig.getConnectionFactory();
        String destination = jmsConfig.getDestination();


        // TODO split up, use more exceptions, add better reporting
        try {
            Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
            props.setProperty(Context.PROVIDER_URL, providerURL);
            javax.naming.Context context = new InitialContext(props);

            // Setup connection
            ConnectionFactory cf = (ConnectionFactory) context.lookup(connectionFactory);
            
            // Setup username/password when required
            Connection connection = null;
            String userName = jmsConfig.getConnectionUserName();
            String password = jmsConfig.getConnectionPassword();
            if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
                connection = cf.createConnection();
            } else {
                connection = cf.createConnection(userName, password);
            }

            // Lookup queue
            Destination dest = (Destination) context.lookup(destination);

            // Create session
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            // Create message producer
            MessageProducer producer = session.createProducer(dest);


            // Create message
            Message message = createMessage(session, content, msgMetaProps, xqcontext);

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

            // Send message
            producer.send(message);

            // Close connection
            // TODO keep connection open for re-use, efficiency
            connection.close();

            return Reporter.createReport(message, jmsConfig);

        } catch (Throwable ex) {
            LOG.error(ex);
            throw new XPathException(ex.getMessage());
        }
    }

    private Message createMessage(Session session, Item item, JmsMessageProperties mdd, XQueryContext xqcontext) throws JMSException, XPathException {

        Message message = null;

        mdd.setProperty(EXIST_XPATH_DATATYPE, Type.getTypeName(item.getType()));

        if (item.getType() == Type.ELEMENT || item.getType() == Type.DOCUMENT) {
            LOG.debug("Streaming element or document node");
            
            mdd.setProperty(EXIST_DATA_TYPE, DATA_TYPE_XML);

            if (item instanceof NodeProxy) {
                NodeProxy np = (NodeProxy) item;
                mdd.setProperty(EXIST_DOCUMENT_URI, np.getDocument().getBaseURI());
                mdd.setProperty(EXIST_DOCUMENT_MIMETYPE, np.getDocument().getMetadata().getMimeType());
            }

            // Node provided
            Serializer serializer = xqcontext.getBroker().newSerializer();

            NodeValue node = (NodeValue) item;
            InputStream is = new NodeInputStream(serializer, node);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                baos.write(is);
                
            } catch (IOException ex) {
                LOG.error(ex);
                throw new XPathException(ex);
            }
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(baos);

            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(baos.toByteArray());

            // Swap
            message = bytesMessage;


        } else if (item.getType() == Type.BASE64_BINARY || item.getType() == Type.HEX_BINARY) {
            
            LOG.debug("Streaming base64 binary");

            if (item instanceof Base64BinaryDocument) {
                Base64BinaryDocument b64doc = (Base64BinaryDocument) item;
                String uri = b64doc.getUrl();

                LOG.debug(String.format("Base64BinaryDocument detected, adding URL %s", uri));
                mdd.setProperty(EXIST_DOCUMENT_URI, uri);
                
            }
            
            mdd.setProperty(EXIST_DATA_TYPE, DATA_TYPE_BINARY);

            BinaryValue binary = (BinaryValue) item;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream is = binary.getInputStream();

            //TODO consider using BinaryValue.getInputStream()
            //byte[] data = (byte[]) binary.toJavaObject(byte[].class);

            try {
                baos.write(is);

            } catch (IOException ex) {
                LOG.error(ex);
                throw new XPathException(ex);
            }
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(baos);

            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(baos.toByteArray());

            // Swap
            message = bytesMessage;

        } else if (item.getType() == Type.STRING) {
            TextMessage textMessage = session.createTextMessage();
            textMessage.setText(item.getStringValue());
            message = textMessage;


        } else {
            ObjectMessage objectMessage = session.createObjectMessage();

            switch (item.getType()) {
                case Type.INTEGER:
                    BigInteger value1 = item.toJavaObject(BigInteger.class);
                    objectMessage.setObject(value1);
                    break;
                case Type.DOUBLE:
                    Double value2 = item.toJavaObject(Double.class);
                    objectMessage.setObject(value2);
                    break;
                case Type.FLOAT:
                    Float value3 = item.toJavaObject(Float.class);
                    objectMessage.setObject(value3);
                    break;
                case Type.DECIMAL:
                    BigDecimal value5 = item.toJavaObject(BigDecimal.class);
                    objectMessage.setObject(value5);
                    break;
                case Type.BOOLEAN:
                    Boolean value6 = item.toJavaObject(Boolean.class);
                    objectMessage.setObject(value6);
                    break;    
                default:
                    throw new XPathException(
                            String.format("Unable to convert '%s' of type '%s' into a JMS object.", item.getStringValue(), item.getType()));
            }

            //objectMessage.setObject(item.toJavaObject(Object.class)); TODO hmmmm
            message = objectMessage;
        }

        return message;
    }

}
