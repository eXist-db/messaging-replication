package org.exist.messaging.misc;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

/**
 * @author wessels
 */
public class MyExceptionListener implements ExceptionListener {

    private final static Logger LOG = LogManager.getLogger();

    @Override
    public void onException(final JMSException jmse) {
        LOG.error(jmse);
    }

}
