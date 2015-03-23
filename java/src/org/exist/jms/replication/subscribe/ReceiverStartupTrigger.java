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
package org.exist.jms.replication.subscribe;

import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.exist.jms.shared.JmsConfiguration;
import org.exist.jms.shared.Receiver;
import org.exist.jms.shared.ReceiversManager;
import org.exist.storage.DBBroker;

/**
 * Startup Trigger to fire-up a message receiver. Typically this trigger is started by
 * configuration in conf.xml
 *
 * @author Dannes Wessels
 */
public class ReceiverStartupTrigger implements org.exist.storage.StartupTrigger {

    private final static Logger LOG = LogManager.getLogger(ReceiverStartupTrigger.class);
   

    /*
     * Entry point for starting the trigger.
     */
    @Override
    public void execute(final DBBroker broker, final Map<String, List<? extends Object>> params) {

        ReceiversManager manager = ReceiversManager.getInstance();
        
        // Get from .xconf file, fill defaults when needed
        SubscriberParameters parameters = new SubscriberParameters();
        parameters.setSingleValueParameters(params);
        
        try {
            // Get parameters, fill defaults when needed
            parameters.processParameters();

            JmsConfiguration jmsConfig = new JmsConfiguration();
            jmsConfig.loadSubscriberParameters(parameters);

            // Setup listeners
            ReplicationJmsListener jmsListener = new ReplicationJmsListener(broker.getBrokerPool());

            Receiver receiver = new Receiver(jmsConfig, jmsListener);
            manager.register(receiver);

            receiver.initialize();
            receiver.start();

            LOG.info("Subscription was successful.");

        } catch (final Throwable t) {            
            LOG.error(String.format("Unable to start subscription: %s", t.getMessage()), t);
        }
    }

}
