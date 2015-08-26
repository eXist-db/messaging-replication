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

import javax.naming.Context;

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
    public void loadConfiguration(AbstractMapType map) throws XPathException {
        // Get all keys
        Sequence keys = map.keys();
        
        // Iterate over all keys
        for (final SequenceIterator i = keys.unorderedIterator(); i.hasNext();) {

            // Get next item
            Item key = i.nextItem();
            
            // Only use Strings as key, as required by JMS
            String keyValue = key.getStringValue();
            
            // Get values
            Sequence values = map.get((AtomicValue)key);

            // Purely set String values
            setProperty(keyValue, values.getStringValue());
        }
    }

    public void loadPublisherParameters(PublisherParameters params) {
        setLocalProperty(Context.INITIAL_CONTEXT_FACTORY, params.getInitialContextFactory());
        setLocalProperty(Context.PROVIDER_URL, params.getProviderUrl());

        setLocalProperty(Constants.CONNECTION_FACTORY, params.getConnectionFactory());
        setLocalProperty(Constants.CLIENT_ID, params.getClientId());
        setLocalProperty(Constants.DESTINATION, params.getDestination());
        setLocalProperty(Constants.PRODUCER_PRIORITY, "" + params.getPriority());
        setLocalProperty(Constants.PRODUCER_TTL, "" + params.getTimeToLive());
        
        setLocalProperty(Constants.JMS_CONNECTION_USERNAME, params.getConnectionUsername());
        setLocalProperty(Constants.JMS_CONNECTION_PASSWORD, params.getConnectionPassword());
    }

    public void loadSubscriberParameters(SubscriberParameters params) {
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

    private void setLocalProperty(String key, String value) {
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
    
    public String getInitialContextFactory(){
        return getProperty(Context.INITIAL_CONTEXT_FACTORY);
    }

    /**
     * Get URL to broker.
     *
     * @see Context#PROVIDER_URL
     * @return URL
     */
    public String getBrokerURL() {
        return getProperty(Context.PROVIDER_URL);
    }
    
    public String getConnectionUserName(){
        return getProperty(Constants.JMS_CONNECTION_USERNAME);
    }
    
    public String getConnectionPassword(){
        return getProperty(Constants.JMS_CONNECTION_PASSWORD);
    }
    
    public String getMessageSelector(){
        return getProperty(Constants.MESSAGE_SELECTOR);
    }
    
    public String getClientId(){
        return getProperty(Constants.CLIENT_ID);
    }

    public Long getTimeToLive() {
        String timeToLiveValue = getProperty(Constants.PRODUCER_TTL);

        Long retVal = null;

        try {
            retVal = NumberUtils.toLong(timeToLiveValue);
        } catch (NumberFormatException ex) {
            LOG.error(ex.getMessage());
        }

        return retVal;

    }

    public Integer getPriority() {
        String priority = getProperty(Constants.PRODUCER_PRIORITY);

        Integer retVal = null;

        try {
            retVal = NumberUtils.toInt(priority);

        } catch (NumberFormatException ex) {
            LOG.error(ex.getMessage());
        }

        return retVal;
    }
    
    /**
     * Verify if all required data is available.
     * 
     * @throws XPathException Data is missing.
     */
    public void validate() throws XPathException {
        
        String initialContextFactory = getInitialContextFactory();
        if(initialContextFactory==null){
            throw new XPathException(String.format(CONFIG_ERROR_MSG, Context.INITIAL_CONTEXT_FACTORY));
        }
        
        String providerURL = getBrokerURL();
        if(providerURL==null){
            throw new XPathException(String.format(CONFIG_ERROR_MSG, Context.PROVIDER_URL));
        }
        
        String connectionFactory = getConnectionFactory();
        if(connectionFactory==null){
            throw new XPathException(String.format(CONFIG_ERROR_MSG, Constants.CONNECTION_FACTORY));
        }
        
        String destination = getDestination();
        if(destination==null){
            throw new XPathException(String.format(CONFIG_ERROR_MSG, Constants.DESTINATION));
        }
        
    }


    /**
     * @return The value or TRUE when not set
     */
    public boolean isDurable() {
        String durable = getProperty(Constants.DURABLE);
        
        if(durable==null){
            return true;
        }
        
        return BooleanUtils.toBoolean(durable);
    }


    public boolean isNoLocal() {
        String noLocal = getProperty(Constants.NO_LOCAL);
        
        if(noLocal==null){
            return true;
        }
        return BooleanUtils.toBoolean(noLocal);
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
