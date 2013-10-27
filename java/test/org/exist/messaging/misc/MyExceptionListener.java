package org.exist.messaging.misc;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import org.apache.log4j.Logger;

/**
 *
 * @author wessels
 */
public class MyExceptionListener implements ExceptionListener {

    private final static Logger LOG = Logger.getLogger(MyExceptionListener.class);

    @Override
    public void onException(JMSException jmse) {
        LOG.error(jmse);
    }

}
