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
 *
 *  $Id$
 */
package org.exist.jms.replication.publish;

import org.apache.commons.lang3.StringUtils;
import org.exist.jms.replication.shared.ClientParameters;
import org.exist.jms.replication.shared.TransportException;
import org.exist.jms.shared.Constants;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.naming.Context;

/**
 * Publisher specific properties.
 *
 * @author Dannes Wessels dannes@exist-db.org
 */
public class PublisherParameters extends ClientParameters {

    private Long timeToLive;
    private Integer priority;
    private String deliveryMode;

    public Long getTimeToLive() {
        return timeToLive;
    }

    public Integer getPriority() {
        return priority;
    }

    public String getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public void processParameters() throws TransportException {

        // Fill defaults when net set
        fillActiveMQbrokerDefaults();

        // java.naming.factory.initial
        String value = props.getProperty(Context.INITIAL_CONTEXT_FACTORY);
        initialContextFactory = value;

        // java.naming.provider.url
        value = props.getProperty(Context.PROVIDER_URL);
        providerUrl = value;

        // Connection factory
        value = props.getProperty(Constants.CONNECTION_FACTORY);
        if (StringUtils.isBlank(value)) {
            value = "ConnectionFactory";
            LOG.info("No " + Constants.CONNECTION_FACTORY + " set, using default value '" + value + "'");
        }
        connectionFactory = value;


        // Setup destination
        value = props.getProperty(Constants.DESTINATION);
        if (StringUtils.isBlank(value)) {
            value = "dynamicTopics/eXistdb";
            LOG.info("No " + Constants.DESTINATION + " set (topic), using default value '" + value
                    + "' which is suitable for activeMQ");
        }
        topic = value;


        // Client ID, when set
        value = props.getProperty(Constants.CLIENT_ID);
        if (StringUtils.isNotBlank(value)) {
            clientId = value;
            LOG.debug(Constants.CLIENT_ID + ": " + value);
        } else {
            LOG.debug(Constants.CLIENT_ID + " is not set.");
        }

        // Get time to live value
        value = props.getProperty(Constants.PRODUCER_TTL);
        if (StringUtils.isNotBlank(value)) {
            try {
                timeToLive = Long.valueOf(value);
            } catch (final NumberFormatException ex) {
                final String errorText = "Unable to set TTL; got '" + value + "'. " + ex.getMessage();
                LOG.error(errorText);
                throw new TransportException(errorText);
            }
        }

        // Get priority
        value = props.getProperty(Constants.PRODUCER_PRIORITY);
        if (StringUtils.isNotBlank(value)) {
            try {
                priority = Integer.valueOf(value);
            } catch (final NumberFormatException ex) {
                final String errorText = String.format("Unable to set priority; got '%s'. %s", value, ex.getMessage());
                LOG.error(errorText);
                throw new TransportException(errorText);
            }
        }

        // Get delivery
        deliveryMode = props.getProperty(Constants.PRODUCER_DELIVERY_MODE);

        // Get connection authentication
        connectionUsername = props.getProperty(Constants.JMS_CONNECTION_USERNAME);
        connectionPassword = props.getProperty(Constants.JMS_CONNECTION_PASSWORD);
    }

    @Override
    public String getReport() {
        return String.format("Publisher configuration: %s='%s' %s='%s' %s='%s' %s='%s' %s='%d' %s='%d'%s='%s'",
                Context.INITIAL_CONTEXT_FACTORY, initialContextFactory, Context.PROVIDER_URL, providerUrl,
                Constants.DESTINATION, topic, Constants.CLIENT_ID, clientId, Constants.PRODUCER_TTL,
                timeToLive, Constants.PRODUCER_PRIORITY, priority, Constants.PRODUCER_DELIVERY_MODE, deliveryMode);
    }
}
