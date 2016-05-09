package org.exist.messaging.misc;

import org.apache.log4j.Logger;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.*;

import javax.jms.*;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Simple JMS listener that echos incoming data to the logger.
 *
 * @author Dannes Wessels
 */
public class MyJMSListener implements MessageListener {

    private final static Logger LOG = Logger.getLogger(MyJMSListener.class);

    @Override
    public void onMessage(Message msg) {

        try {
            LOG.info(String.format("msgId='%s'", msg.getJMSMessageID()));

            // Show content
            String content = null;
            if (msg instanceof TextMessage) {
                LOG.info("TextMessage");
                content = ((TextMessage) msg).getText();

            } else if (msg instanceof BytesMessage) {
                BytesMessage bm = (BytesMessage) msg;
                LOG.info("nrbytes" + bm.getBodyLength());
                content = "muted";

            } else if (msg instanceof ObjectMessage) {
                Object obj = ((ObjectMessage) msg).getObject();
                LOG.info(obj.getClass().getCanonicalName());

                if (obj instanceof BigInteger) {
                    IntegerValue value = new IntegerValue((BigInteger) obj);
                    content = value.getStringValue();

                } else if (obj instanceof Double) {
                    DoubleValue value = new DoubleValue((Double) obj);
                    content = value.getStringValue();

                } else if (obj instanceof BigDecimal) {
                    DecimalValue value = new DecimalValue((BigDecimal) obj);
                    content = value.getStringValue();

                } else if (obj instanceof Boolean) {
                    BooleanValue value = new BooleanValue((Boolean) obj);
                    content = value.getStringValue();

                } else if (obj instanceof Float) {
                    FloatValue value = new FloatValue((Float) obj);
                    content = value.getStringValue();

                } else {
                    LOG.error(String.format("Unable to convert %s", obj.toString()));
                }

            } else {
                LOG.info(msg.getClass().getCanonicalName());
                content = msg.toString();
            }
            LOG.info(String.format("content='%s' type='%s'", content, msg.getJMSType()));

//            // Log properties
//            Enumeration props = msg.getPropertyNames();
//            while (props.hasMoreElements()) {
//                String elt = (String) props.nextElement();
//                String value = msg.getStringProperty(elt);
//                LOG.info(String.format("'%s'='%s'", elt, value));
//            }

        } catch (JMSException ex) {
            LOG.error(ex);

        } catch (XPathException ex) {
            LOG.error(ex);

        } catch (Throwable ex) {
            LOG.error(ex);
        }
    }
}
