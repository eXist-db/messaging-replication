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
package org.exist.jms.shared;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.math.NumberUtils;
import org.exist.jms.replication.publish.PublisherParameters;
import org.exist.jms.replication.subscribe.SubscriberParameters;
import org.exist.xquery.XPathException;
import org.exist.xquery.functions.map.AbstractMapType;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.naming.Context;

import static org.exist.jms.shared.ErrorCodes.JMS011;


/**
 * Wrapper for managing JMS configuration items.
 *
 * @author Dannes Wessels
 */
public class JmsConfiguration extends MessagingConfiguration {

    private static final String CONFIG_ERROR_MSG = "Missing configuration item '%s'";

    /**
     * Load data from XQuery map-type and convert the data into String key/value pairs.
     *
     * @param map The XQuery map
     * @throws XPathException Something bad happened.
     */
    @Override
    public void loadConfiguration(final AbstractMapType map) throws XPathException {
        // Get all keys
        final Sequence keys = map.keys();

        // Iterate over all keys
        for (final SequenceIterator i = keys.unorderedIterator(); i.hasNext(); ) {

            // Get next item
            final Item key = i.nextItem();

            // Only use Strings as key, as required by JMS
            final String keyValue = key.getStringValue();

            // Get values
            final Sequence values = map.get((AtomicValue) key);

            // Purely set String values
            setProperty(keyValue, values.getStringValue());
        }
    }

    public void loadPublisherParameters(final PublisherParameters params) {
        setLocalProperty(Context.INITIAL_CONTEXT_FACTORY, params.getInitialContextFactory());
        setLocalProperty(Context.PROVIDER_URL, params.getProviderUrl());

        setLocalProperty(Constants.CONNECTION_FACTORY, params.getConnectionFactory());
        setLocalProperty(Constants.CLIENT_ID, params.getClientId());
        setLocalProperty(Constants.DESTINATION, params.getDestination());
        setLocalProperty(Constants.PRODUCER_PRIORITY, "" + params.getPriority());
        setLocalProperty(Constants.PRODUCER_TTL, "" + params.getTimeToLive());
        setLocalProperty(Constants.PRODUCER_DELIVERY_MODE, params.getDeliveryMode());

        setLocalProperty(Constants.JMS_CONNECTION_USERNAME, params.getConnectionUsername());
        setLocalProperty(Constants.JMS_CONNECTION_PASSWORD, params.getConnectionPassword());
    }

    public void loadSubscriberParameters(final SubscriberParameters params) {
        setLocalProperty(Context.INITIAL_CONTEXT_FACTORY, params.getInitialContextFactory());
        setLocalProperty(Context.PROVIDER_URL, params.getProviderUrl());

        setLocalProperty(Constants.CONNECTION_FACTORY, params.getConnectionFactory());
        setLocalProperty(Constants.CLIENT_ID, params.getClientId());
        setLocalProperty(Constants.DESTINATION, params.getDestination());
        setLocalProperty(Constants.DURABLE, "" + params.isDurable());
        setLocalProperty(Constants.MESSAGE_SELECTOR, params.getMessageSelector());
        setLocalProperty(Constants.NO_LOCAL, "" + params.isNoLocal());
        setLocalProperty(Constants.SUBSCRIBER_NAME, params.getSubscriberName());

        setLocalProperty(Constants.JMS_CONNECTION_USERNAME, params.getConnectionUsername());
        setLocalProperty(Constants.JMS_CONNECTION_PASSWORD, params.getConnectionPassword());
    }

    private void setLocalProperty(final String key, final String value) {
        if (StringUtils.isNotBlank(key) && value != null) {
            setProperty(key, value);
        }
    }


    public String getConnectionFactory() {
        return getProperty(Constants.CONNECTION_FACTORY);
    }

    public String getDestination() {
        return getProperty(Constants.DESTINATION);
    }

    public String getInitialContextFactory() {
        return getProperty(Context.INITIAL_CONTEXT_FACTORY);
    }

    /**
     * Get URL to broker.
     *
     * @return URL
     * @see Context#PROVIDER_URL
     */
    public String getBrokerURL() {
        return getProperty(Context.PROVIDER_URL);
    }

    public String getConnectionUserName() {
        return getProperty(Constants.JMS_CONNECTION_USERNAME);
    }

    public String getConnectionPassword() {
        return getProperty(Constants.JMS_CONNECTION_PASSWORD);
    }

    public String getMessageSelector() {
        return getProperty(Constants.MESSAGE_SELECTOR);
    }

    public String getClientId() {
        return getProperty(Constants.CLIENT_ID);
    }

    public Long getTimeToLive() {
        final String timeToLiveValue = getProperty(Constants.PRODUCER_TTL);

        Long retVal = null;

        try {
            retVal = NumberUtils.toLong(timeToLiveValue);
        } catch (final NumberFormatException ex) {
            LOG.error(ex.getMessage());
        }

        return retVal;

    }

    public Integer getPriority() {
        final String priority = getProperty(Constants.PRODUCER_PRIORITY);

        Integer retVal = null;

        try {
            retVal = NumberUtils.toInt(priority);

        } catch (final NumberFormatException ex) {
            LOG.error(ex.getMessage());
        }

        return retVal;
    }

    public Integer getDeliveryMethod() {
        final String mode = getProperty(Constants.PRODUCER_DELIVERY_MODE);

        if (StringUtils.isBlank(mode)) {
            return null;
        }

        Integer deliveryMode = null;
        switch (mode.toLowerCase()) {
            case "persistent":
                deliveryMode = DeliveryMode.PERSISTENT;
                break;
            case "non-persistent":
                deliveryMode = DeliveryMode.NON_PERSISTENT;
                break;
            case "default":
                deliveryMode = Message.DEFAULT_DELIVERY_MODE;
                break;
            default:
                LOG.error("Value '{}' is not supported as value for DeliveryMode", mode);
                break;
        }
        return deliveryMode;
    }

    /**
     * Verify if all required data is available.
     *
     * @throws XPathException Data is missing.
     */
    public void validate() throws XPathException {

        final String initialContextFactory = getInitialContextFactory();
        if (initialContextFactory == null) {
            throw new XPathException(JMS011, String.format(CONFIG_ERROR_MSG, Context.INITIAL_CONTEXT_FACTORY));
        }

        final String providerURL = getBrokerURL();
        if (providerURL == null) {
            throw new XPathException(JMS011, String.format(CONFIG_ERROR_MSG, Context.PROVIDER_URL));
        }

        final String connectionFactory = getConnectionFactory();
        if (connectionFactory == null) {
            throw new XPathException(JMS011, String.format(CONFIG_ERROR_MSG, Constants.CONNECTION_FACTORY));
        }

        final String destination = getDestination();
        if (destination == null) {
            throw new XPathException(JMS011, String.format(CONFIG_ERROR_MSG, Constants.DESTINATION));
        }

    }


    /**
     * @return The value or TRUE when not set
     */
    public boolean isDurable() {
        final String durable = getProperty(Constants.DURABLE);

        return durable == null || BooleanUtils.toBoolean(durable);

    }


    public boolean isNoLocal() {
        final String noLocal = getProperty(Constants.NO_LOCAL);

        return noLocal == null || BooleanUtils.toBoolean(noLocal);
    }


    public String getSubscriberName() {
        return getProperty(Constants.SUBSCRIBER_NAME);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append(Context.INITIAL_CONTEXT_FACTORY, getInitialContextFactory())
                .append(Context.PROVIDER_URL, getBrokerURL())
                .append(Constants.CLIENT_ID, getClientId())
                .append(Constants.CONNECTION_FACTORY, getConnectionFactory())
                .append(Constants.JMS_CONNECTION_USERNAME, getConnectionUserName())
                .append(Constants.DESTINATION, getDestination())
                .append(Constants.MESSAGE_SELECTOR, getMessageSelector())
                .append(Constants.PRODUCER_PRIORITY, getPriority())
                .append(Constants.PRODUCER_TTL, getTimeToLive())
                .append(Constants.DURABLE, isDurable())
                .append(Constants.NO_LOCAL, isNoLocal())
                .toString();
    }

}
