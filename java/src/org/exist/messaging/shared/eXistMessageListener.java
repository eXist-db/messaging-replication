package org.exist.messaging.shared;

import javax.jms.MessageListener;

/**
 *
 * @author wessels
 */
public interface eXistMessageListener extends MessageListener {

    public Report getReport();

    public String getUsageType();
    
}
