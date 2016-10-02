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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.ConnectionFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for creating and buffering ConnectionFactory instances.
 *
 * @author Dannes Wessels
 */
class SenderConnectionFactory {

    public static final String ACTIVEMQ_POOLED_CONNECTION_FACTORY = "org.apache.activemq.pool.PooledConnectionFactory";
    private final static Logger LOG = LogManager.getLogger(SenderConnectionFactory.class);
    private static final Map<String, ConnectionFactory> connectionFactories = new HashMap<>();

    /**
     * Get Connection Factory. Return existing factory or create new one if not existent.
     *
     * @param brokerURL URL to broker
     * @param poolParam name of specific connection factory, or yes or true or activemq to use optimized pooled activemq factory.
     * @return the connection factory
     */
    static ConnectionFactory getConnectionFactoryInstance(final String brokerURL, String poolParam) {

        final String storeID = brokerURL + "#" + poolParam;

        // Try to get CF
        ConnectionFactory retVal = connectionFactories.get(storeID);

        // WHen not available create a new CF
        if (retVal == null) {
            try {
                LOG.info("Creating new connection factory for {}", brokerURL);

                if (StringUtils.isBlank(poolParam) || "yes".equalsIgnoreCase(poolParam)
                        || "true".equalsIgnoreCase(poolParam) || "activemq".equalsIgnoreCase(poolParam)) {
                    poolParam = ACTIVEMQ_POOLED_CONNECTION_FACTORY;
                }

                LOG.info("Connection factory: {}", poolParam);

                // Construct and initialize the factory
                final Class<?> clazz = Class.forName(poolParam);
                final Object object = ConstructorUtils.invokeConstructor(clazz, brokerURL);

                // Convert to class
                final ConnectionFactory cf = (ConnectionFactory) object;

                // Store newly created factory
                connectionFactories.put(storeID, cf);

                // Return to requester
                retVal = cf;

            } catch (final Throwable t) {
                LOG.error("Unable to create new connection factory: {}", t.getMessage());
            }

        } else {
            LOG.debug("Reusing existing connectionFactory for {}", brokerURL);
        }

        return retVal;
    }

}
