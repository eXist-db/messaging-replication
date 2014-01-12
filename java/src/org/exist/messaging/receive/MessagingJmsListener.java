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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import org.exist.EXistException;
import org.exist.Namespaces;
import org.exist.memtree.DocumentImpl;
import org.exist.memtree.SAXAdapter;

import static org.exist.messaging.shared.Constants.COMPRESSION_TYPE_GZIP;
import static org.exist.messaging.shared.Constants.DATA_TYPE_XML;
import static org.exist.messaging.shared.Constants.EXIST_DATA_TYPE;
import static org.exist.messaging.shared.Constants.EXIST_DOCUMENT_COMPRESSION;
import static org.exist.messaging.shared.Constants.JMS_CORRELATION_ID;
import static org.exist.messaging.shared.Constants.JMS_EXPIRATION;
import static org.exist.messaging.shared.Constants.JMS_MESSAGE_ID;
import static org.exist.messaging.shared.Constants.JMS_PRIORITY;
import static org.exist.messaging.shared.Constants.JMS_TIMESTAMP;
import static org.exist.messaging.shared.Constants.JMS_TYPE;

import org.exist.messaging.shared.Report;
import org.exist.messaging.shared.eXistMessageListener;
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
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * JMS message receiver. Passes call to XQuery callback function.
 *
 * @author Dannes Wessels
 */
public class MessagingJmsListener implements eXistMessageListener {

    private final static Logger LOG = Logger.getLogger(MessagingJmsListener.class);
    private final FunctionReference functionReference;
    private final XQueryContext xqueryContext;
    private final Sequence functionParams;
    
    private final Report report = new Report();
    
    private Session session;
    
    public void setSession(Session session){
        this.session=session;
    }

    public MessagingJmsListener(FunctionReference functionReference, Sequence functionParams, XQueryContext xqueryContext) {
        super();
        this.functionReference = functionReference;
        this.xqueryContext = xqueryContext;
        this.functionParams = functionParams;
    }

    @Override
    public void onMessage(Message msg) {

        report.start();

        // Log incoming message
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Received message: ID=%s JavaType=%s", msg.getJMSMessageID(), msg.getClass().getSimpleName()));
            } else {
                LOG.info(String.format("Received message: ID=%s", msg.getJMSMessageID()));
            }

        } catch (JMSException ex) {
            report.add(ex.getMessage());
            LOG.error(ex.getMessage());
        }

        // Placeholder for broker
        DBBroker dummyBroker = null;
        BrokerPool brokerPool = null;

        try {
            /*
             * Work around to to have a broker available for the
             * execution of #evalFunction. In the onMessage() method this
             * broker is not being used at all.
             */
            brokerPool = BrokerPool.getInstance();
            dummyBroker = brokerPool.getBroker();

            // Copy message and jms configuration details into Maptypes
            MapType msgProperties = getMessageProperties(msg, xqueryContext);
            MapType jmsProperties = getJmsProperties(msg, xqueryContext);

            // This sequence shall contain the actual conten that will be passed
            // to the callback function
            Sequence content = null;

            // Switch based on incoming
            if (msg instanceof TextMessage) {

                // xs:string values are passed as regular Text messages
                content = new StringValue(((TextMessage) msg).getText());

            } else if (msg instanceof ObjectMessage) {

                // the supported other types are wrapped into a corresponding
                // Java object inside the ObjectMessage
                content = handleObjectMessage((ObjectMessage) msg);

            } else if (msg instanceof BytesMessage) {

                // XML nodes and base64 (binary) data are sent as an array of bytes
                BytesMessage bm = (BytesMessage) msg;

                // Read data into byte buffer
                byte[] data = new byte[(int) bm.getBodyLength()];
                bm.readBytes(data);

                String value = msg.getStringProperty(EXIST_DOCUMENT_COMPRESSION);
                boolean isCompressed = (StringUtils.isNotBlank(value) && COMPRESSION_TYPE_GZIP.equals(value));

                // Serialize data
                if (DATA_TYPE_XML.equalsIgnoreCase(bm.getStringProperty(EXIST_DATA_TYPE))) {
                    // XML(fragment)
                    content = processXML(data, isCompressed);

                } else {
                    // Binary data
                    InputStream is = new ByteArrayInputStream(data);

                    if (isCompressed) {
                        is = new GZIPInputStream(is);
                    }
                    BinaryValue bv = Base64BinaryDocument.getInstance(xqueryContext, is);
                    content = bv;
                    IOUtils.closeQuietly(is);
                }

            } else {
                // Unsupported JMS message type
                String txt = String.format("Unsupported JMS Message type %s", msg.getClass().getCanonicalName());
                report.add(txt);
                LOG.error(txt);
                throw new XPathException(txt);
            }

            // Setup parameters callback function
            Sequence params[] = new Sequence[4];
            params[0] = content;
            params[1] = functionParams;
            params[2] = msgProperties;
            params[3] = jmsProperties;

            // Execute callback function
            LOG.info("Eval");
System.out.println("callEval----");
            Sequence result = functionReference.evalFunction(null, null, params);
System.out.println("callEval done " + result);

            // Done
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Function returned %s", result.getStringValue()));
            }

            // Acknowledge processing
System.out.println("ack");
            msg.acknowledge();

            // Update statistics
            report.incMessageCounterOK();

        } catch (JMSException ex) {
            report.add(ex.getMessage());
            LOG.error(ex.getMessage(), ex);
            try {
                session.close();
            } catch (JMSException ex1) {
                System.out.println(ex1.getMessage());
            }
//            throw new RuntimeException(ex.getMessage());

        } catch (XPathException ex) {
            report.add(ex.getMessage());
            LOG.error(ex);
            try {
                session.close();
            } catch (JMSException ex1) {
                System.out.println(ex1.getMessage());
            }
//            throw new RuntimeException(ex.getMessage());

        } catch (IOException ex) {
            report.add(ex.getMessage());
            LOG.error(ex.getMessage(), ex);
            try {
                session.close();
            } catch (JMSException ex1) {
                System.out.println(ex1.getMessage());
            }
//            throw new RuntimeException(ex.getMessage());

       
        } catch (EXistException ex) {
            report.add(ex.getMessage());
            LOG.error(ex.getMessage(), ex);
            try {
                session.close();
            } catch (JMSException ex1) {
                System.out.println(ex1.getMessage());
            }
//            throw new RuntimeException(ex.getMessage());
} catch (Throwable ex) {
            report.add(ex.getMessage());
            LOG.error(ex.getMessage(), ex);
            try {
                session.close();
            } catch (JMSException ex1) {
                System.out.println(ex1.getMessage());
            }
//            throw new RuntimeException(ex.getMessage());

        } finally {
            // Cleanup resources
            if (dummyBroker != null && brokerPool != null) {
                brokerPool.release(dummyBroker);
            }
            
            // update statistics
            report.stop();
            report.incMessageCounterTotal();
            report.addCumulatedProcessingTime();
        }

    }


    private MapType getMessageProperties(Message msg, XQueryContext xqueryContext) throws XPathException, JMSException {
        // Copy property values into Maptype
        MapType map = new MapType(xqueryContext);

        Enumeration props = msg.getPropertyNames();
        while (props.hasMoreElements()) {
            String key = (String) props.nextElement();
            
            Object obj = msg.getObjectProperty(key);

            if(obj instanceof String){
                String value = msg.getStringProperty(key);
                addStringKV(map, key, value);
                
            } else if (obj instanceof Integer) {
                Integer localValue = msg.getIntProperty(key);
                ValueSequence vs = new ValueSequence(new IntegerValue(localValue));
                addKV(map, key, vs);
   
            } else if (obj instanceof Double) {
                Double localValue = msg.getDoubleProperty(key);
                ValueSequence vs = new ValueSequence(new DoubleValue(localValue));
                addKV(map, key, vs);
            
            } else if (obj instanceof Boolean) {   
                Boolean localValue = msg.getBooleanProperty(key);
                ValueSequence vs = new ValueSequence(new BooleanValue(localValue));
                addKV(map, key, vs);
                
            } else if (obj instanceof Float) {
                Float localValue = msg.getFloatProperty(key);
                ValueSequence vs = new ValueSequence(new FloatValue(localValue));
                addKV(map, key, vs);
                
            } else {             
                String value = msg.getStringProperty(key);
                addStringKV(map, key, value);
                
                if(LOG.isDebugEnabled()){
                    LOG.debug(String.format("Unable to convert '%s'/'%s' into a map. Falling back to String value", key, value));
                }
            }

        }
        return map;
    }

    private MapType getJmsProperties(Message msg, XQueryContext xqueryContext) throws XPathException, JMSException {
        // Copy property values into Maptype
        MapType map = new MapType(xqueryContext);

        addStringKV(map, JMS_MESSAGE_ID, msg.getJMSMessageID());
        addStringKV(map, JMS_CORRELATION_ID, msg.getJMSCorrelationID());
        addStringKV(map, JMS_TYPE, msg.getJMSType());
        addStringKV(map, JMS_PRIORITY, "" + msg.getJMSPriority());
        addStringKV(map, JMS_EXPIRATION, "" + msg.getJMSExpiration());
        addStringKV(map, JMS_TIMESTAMP, "" + msg.getJMSTimestamp());

        return map;
    }

    private void addStringKV(MapType map, String key, String value) throws XPathException {
        if (map != null && key != null && !key.isEmpty() && value != null) {
            map.add(new StringValue(key), new ValueSequence(new StringValue(value)));
        }
    }
    
    private void addKV(MapType map, String key, ValueSequence valueSequence) throws XPathException {
        if (map != null && StringUtils.isNotBlank(key) && valueSequence != null) {
            map.add(new StringValue(key), valueSequence);    
        }
    }

    /**
     * @throws JMSException if the JMS provider fails to set the object due to some internal error.
     * @throws XPathException Object type is not supported.
     */
    private Sequence handleObjectMessage(ObjectMessage msg) throws JMSException, XPathException {

        Object obj = ((ObjectMessage) msg).getObject();
        Sequence content = null;

        if (obj instanceof BigInteger) {
            content = new IntegerValue((BigInteger) obj);

        } else if (obj instanceof Double) {
            content = new DoubleValue((Double) obj);

        } else if (obj instanceof BigDecimal) {
            content = new DecimalValue((BigDecimal) obj);

        } else if (obj instanceof Boolean) {
            content = new BooleanValue((Boolean) obj);

        } else if (obj instanceof Float) {
            content = new FloatValue((Float) obj);

        } else {
            String txt = String.format("Unable to convert the object %s", obj.toString());
            report.add(txt);
            LOG.error(txt);
            throw new XPathException(txt);
        }
        return content;
    }

    private Sequence processXML(byte[] data, boolean isGzipped) throws XPathException {

        Sequence content = null;
        try {
            // Reading compressed XML fragment
            InputStream is = new ByteArrayInputStream(data);

            // Decompress when needed
            if (isGzipped) {
                is = new GZIPInputStream(is);
            }

            final ValidationReport report = new ValidationReport();
            final SAXAdapter adapter = new SAXAdapter(xqueryContext);
            final SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            final InputSource src = new InputSource(is);
            final SAXParser parser = factory.newSAXParser();
            XMLReader xr = parser.getXMLReader();

            xr.setErrorHandler(report);
            xr.setContentHandler(adapter);
            xr.setProperty(Namespaces.SAX_LEXICAL_HANDLER, adapter);

            xr.parse(src);

            // Cleanup
            IOUtils.closeQuietly(is);

            if (report.isValid()) {
                content = (DocumentImpl) adapter.getDocument();
            } else {
                String txt = String.format("Received document is not valid: %s", report.toString());
                LOG.debug(txt);
                throw new XPathException(txt);
            }

        } catch (SAXException ex) {
            report.add(ex.getMessage());
            throw new XPathException(ex.getMessage());

        } catch (ParserConfigurationException ex) {
            report.add(ex.getMessage());
            throw new XPathException(ex.getMessage());

        } catch (IOException ex) {
            report.add(ex.getMessage());
            throw new XPathException(ex.getMessage());
        }

        return content;
    }

    public Report getReport() {
        return report;
    }

    @Override
    public String getUsageType() {
        return "messaging";
    }
}
