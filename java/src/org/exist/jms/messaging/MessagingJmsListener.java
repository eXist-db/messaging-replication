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
package org.exist.jms.messaging;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.Namespaces;
import org.exist.dom.memtree.SAXAdapter;
import org.exist.jms.shared.Report;
import org.exist.jms.shared.eXistMessagingListener;
import org.exist.security.Subject;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.validation.ValidationReport;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.jms.*;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Enumeration;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import static org.exist.jms.shared.Constants.*;

/**
 * JMS message receiver. Passes call to XQuery callback function.
 *
 * @author Dannes Wessels
 */
public class MessagingJmsListener extends eXistMessagingListener {

    private final static Logger LOG = LogManager.getLogger(MessagingJmsListener.class);
    private final FunctionReference functionReference;
    private final XQueryContext xqueryContext;
    private final Sequence functionParams;
    private Subject subject;
    private Report report = null;

    //    private Session session;
    private int receiverID = -1;


//    public void setSession(Session session){
//        this.session=session;
//    }
//    
//    public void setReceiverID(String receiverID){
//        this.receiverID=receiverID;
//    }

    public MessagingJmsListener(final Subject subject, final FunctionReference functionReference, final Sequence functionParams, final XQueryContext xqueryContext) {
        super();
        this.functionReference = functionReference;
        this.xqueryContext = xqueryContext.copyContext();
        this.functionParams = functionParams;
        this.report = getReport();
        this.subject = subject;
    }

    @Override
    public void onMessage(final Message msg) {

        // Make a copy, just in case
        XQueryContext copyContext = xqueryContext.copyContext();

        // Set new context to function reference
        functionReference.setContext(copyContext);

        receiverID = getReceiverID();

        report.start();

        // Log incoming message
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received message: receiverID={} messageId={} javaClass={}", receiverID, msg.getJMSMessageID(), msg.getClass().getSimpleName());
            } else {
                LOG.info("Received message: receiverID={} messageId={}", receiverID, msg.getJMSMessageID());
            }

        } catch (final JMSException ex) {
            report.addListenerError(ex);
            LOG.error(String.format("%s (Receiver=%s)", ex.getMessage(), receiverID), ex);
        }

        // Placeholder for broker
        BrokerPool brokerPool = null;

        try {
            /*
             * Work around to to have a broker available for the
             * execution of #evalFunction. In the onMessage() method this
             * broker is not being used at all.
             */
            brokerPool = BrokerPool.getInstance();

            // Actually the subject in the next line influences the subject used 
            // executing the callback function. Must be same userid in which
            // the query was started.
            if (subject == null) {
                subject = brokerPool.getSecurityManager().getGuestSubject();
            }

            try (DBBroker dummyBroker = brokerPool.get(Optional.of(subject))) {


                // Copy message and jms configuration details into Maptypes
                final MapType msgProperties = getMessageProperties(msg, xqueryContext);
                final MapType jmsProperties = getJmsProperties(msg, xqueryContext);

                // Retrieve content of message
                final Sequence content = getContent(msg);

                // Setup parameters callback function
                final Sequence[] params = new Sequence[4];
                params[0] = content;
                params[1] = functionParams;
                params[2] = msgProperties;
                params[3] = jmsProperties;

                // Execute callback function
                LOG.debug("Receiver={} : call evalFunction", receiverID);
                final Sequence result = functionReference.evalFunction(null, null, params);

                // Done
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Receiver={} : Function returned %s", receiverID, result.getStringValue());
                }

                // Acknowledge processing
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Receiver={} : call acknowledge", receiverID);
                }
                msg.acknowledge();

                // Update statistics
                report.incMessageCounterOK();
            }

        } catch (final Throwable ex) {

            LOG.error(ex.getMessage(), ex);

            report.addListenerError(ex);
            LOG.error(String.format("%s (Receiver=%s)", ex.getMessage(), receiverID), ex);

            final Session session = getSession();
            try {
                if (session != null) {
                    session.close();
                }
            } catch (final JMSException ex1) {
                LOG.error(String.format("%s (Receiver=%s)", ex1.getMessage(), receiverID), ex1);
            }


        } finally {

            // update statistics
            report.stop();
            report.incMessageCounterTotal();
            report.addCumulatedProcessingTime();
        }

    }

    /**
     * Convert JMS message into a sequence of data.
     *
     * @param msg The JMS message object
     * @return Sequence representing the JMS message
     * @throws IOException    An internal IO error occurred.
     * @throws XPathException An eXist-db object could not be  handled.
     * @throws JMSException   A problem occurred handling an JMS object.
     */
    private Sequence getContent(final Message msg) throws IOException, XPathException, JMSException {
        // This sequence shall contain the actual conten that will be passed
        // to the callback function
        Sequence content = null;

        // Switch based on type incoming object
        if (msg instanceof TextMessage) {

            // xs:string values are passed as regular Text messages
            content = new StringValue(((TextMessage) msg).getText());

        } else if (msg instanceof ObjectMessage) {

            // the supported other types are wrapped into a corresponding
            // Java object inside the ObjectMessage
            content = handleObjectMessage((ObjectMessage) msg);

        } else if (msg instanceof BytesMessage) {

            // XML nodes and base64 (binary) data are sent as an array of bytes
            final BytesMessage bm = (BytesMessage) msg;

            // Read data into byte buffer
            final byte[] data = new byte[(int) bm.getBodyLength()];
            bm.readBytes(data);

            final String value = msg.getStringProperty(EXIST_DOCUMENT_COMPRESSION);
            final boolean isCompressed = (StringUtils.isNotBlank(value) && COMPRESSION_TYPE_GZIP.equals(value));

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
                content = Base64BinaryDocument.getInstance(xqueryContext, is);
                IOUtils.closeQuietly(is);
            }

        } else {
            // Unsupported JMS message type
            final String txt = String.format("Unsupported JMS Message type %s", msg.getClass().getCanonicalName());

            final XPathException ex = new XPathException(txt);
            report.addListenerError(ex);
            LOG.error(txt);
            throw ex;
        }
        return content;
    }

    /**
     * Convert JMS message properties into an eXist-db map.
     *
     * @param msg           The JMS message
     * @param xqueryContext eXist-db query context
     * @return eXist-db map containing the properties
     */
    private MapType getMessageProperties(final Message msg, final XQueryContext xqueryContext) throws XPathException, JMSException {
        // Copy property values into Maptype
        final MapType map = new MapType(xqueryContext);

        final Enumeration props = msg.getPropertyNames();
        while (props.hasMoreElements()) {
            final String key = (String) props.nextElement();

            final Object obj = msg.getObjectProperty(key);

            if (obj instanceof String) {
                final String value = msg.getStringProperty(key);
                addStringKV(map, key, value);

            } else if (obj instanceof Integer) {
                final Integer localValue = msg.getIntProperty(key);
                final ValueSequence vs = new ValueSequence(new IntegerValue(localValue));
                addKV(map, key, vs);

            } else if (obj instanceof Double) {
                final Double localValue = msg.getDoubleProperty(key);
                final ValueSequence vs = new ValueSequence(new DoubleValue(localValue));
                addKV(map, key, vs);

            } else if (obj instanceof Boolean) {
                final Boolean localValue = msg.getBooleanProperty(key);
                final ValueSequence vs = new ValueSequence(new BooleanValue(localValue));
                addKV(map, key, vs);

            } else if (obj instanceof Float) {
                final Float localValue = msg.getFloatProperty(key);
                final ValueSequence vs = new ValueSequence(new FloatValue(localValue));
                addKV(map, key, vs);

            } else {
                final String value = msg.getStringProperty(key);
                addStringKV(map, key, value);

                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Unable to convert '%s'/'%s' into a map. Falling back to String value", key, value));
                }
            }

        }
        return map;
    }

    /**
     * Convert JMS connection properties into an eXist-db map.
     *
     * @param msg           The JMS message
     * @param xqueryContext eXist-db query context
     * @return eXist-db map containing the properties
     */
    private MapType getJmsProperties(final Message msg, final XQueryContext xqueryContext) throws XPathException, JMSException {
        // Copy property values into Maptype
        final MapType map = new MapType(xqueryContext);

        addStringKV(map, JMS_MESSAGE_ID, msg.getJMSMessageID());
        addStringKV(map, JMS_CORRELATION_ID, msg.getJMSCorrelationID());
        addStringKV(map, JMS_TYPE, msg.getJMSType());
        addStringKV(map, JMS_PRIORITY, "" + msg.getJMSPriority());
        addStringKV(map, JMS_EXPIRATION, "" + msg.getJMSExpiration());
        addStringKV(map, JMS_TIMESTAMP, "" + msg.getJMSTimestamp());

        return map;
    }

    /**
     * Add key-value pair to map.
     *
     * @param map   Target for key-value data.
     * @param key   The key value to retrieve the data.
     * @param value Data corresponding to the key.
     * @throws XPathException A map operation failed.
     */
    private void addStringKV(final MapType map, final String key, final String value) throws XPathException {
        if (map != null && key != null && !key.isEmpty() && value != null) {
            map.add(new StringValue(key), new ValueSequence(new StringValue(value)));
        }
    }

    /**
     * Add key-value pair to map.
     *
     * @param map           Target for key-value data.
     * @param key           The key value to retrieve the data.
     * @param valueSequence Data corresponding to the key.
     * @throws XPathException A map operation failed.
     */
    private void addKV(final MapType map, final String key, final ValueSequence valueSequence) throws XPathException {
        if (map != null && StringUtils.isNotBlank(key) && valueSequence != null) {
            map.add(new StringValue(key), valueSequence);
        }
    }

    /**
     * Convert JMS' objectmessage into an Xquery sequence with one value.
     *
     * @throws JMSException   if the JMS provider fails to set the object due to some internal error.
     * @throws XPathException Object type is not supported.
     */
    private Sequence handleObjectMessage(final ObjectMessage msg) throws JMSException, XPathException {

        final Object obj = msg.getObject();
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
            final String txt = String.format("Unable to convert the object %s", obj.toString());

            final XPathException ex = new XPathException(txt);
            report.addListenerError(ex);
            LOG.error(txt);
            throw ex;
        }
        return content;
    }

    /**
     * Parse an byte-array containing (compressed) XML data into
     * an eXist-db document.
     *
     * @param data      Byte array containg the XML data.
     * @param isGzipped Set TRUE is data is in GZIP format
     * @return Sequence containing the XML as DocumentImpl
     * @throws XPathException Something bad happened.
     */
    private Sequence processXML(final byte[] data, final boolean isGzipped) throws XPathException {

        Sequence content = null;
        try {
            // Reading compressed XML fragment
            InputStream is = new ByteArrayInputStream(data);

            // Decompress when needed
            if (isGzipped) {
                is = new GZIPInputStream(is);
            }

            final ValidationReport validationReport = new ValidationReport();
            final SAXAdapter adapter = new SAXAdapter(xqueryContext);
            final SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            final InputSource src = new InputSource(is);
            final SAXParser parser = factory.newSAXParser();
            final XMLReader xr = parser.getXMLReader();

            xr.setErrorHandler(validationReport);
            xr.setContentHandler(adapter);
            xr.setProperty(Namespaces.SAX_LEXICAL_HANDLER, adapter);

            xr.parse(src);

            // Cleanup
            IOUtils.closeQuietly(is);

            if (validationReport.isValid()) {
                content = adapter.getDocument();
            } else {
                final String txt = String.format("Received document is not valid: %s", validationReport.toString());
                LOG.debug(txt);
                throw new XPathException(txt);
            }

        } catch (SAXException | ParserConfigurationException | IOException ex) {
            report.addListenerError(ex);
            throw new XPathException(ex.getMessage());

        }

        return content;
    }


    @Override
    public String getUsageType() {
        return "messaging";
    }

}
