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
package org.exist.messaging.configuration;

import javax.naming.Context;

import org.exist.messaging.shared.Constants;

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
    
    public static final String CONFIG_ERROR_MSG = "Missing configuration item '%s'";
    
    /**
     * Load data from XQuery map-type and convert the data into String key/value pairs.
     *
     * @param map The XQuery map
     * @return The converted map
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

    
    public String getConnectionFactory() {
        return getProperty(Constants.CONNECTION_FACTORY);
    }

    public String getDestination() {
        return getProperty(Constants.DESTINATION);
    }
    
    public String getInitialContextFactory(){
        return getProperty(Context.INITIAL_CONTEXT_FACTORY);
    }
    
    public String getProviderURL(){
        return getProperty(Context.PROVIDER_URL);
    }
    
    public String getConnectionUserName(){
        return getProperty(Constants.JMS_CONNECTION_USERNAME);
    }
    
    public String getConnectionPassword(){
        return getProperty(Constants.JMS_CONNECTION_PASSWORD);
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
        
        String providerURL = getProviderURL();
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
}
