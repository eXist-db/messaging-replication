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
import org.exist.xquery.XPathException;


/**
 * Wrapper for managing JMS configuration items.
 * 
 * @author Dannes Wessels
 */
public class JmsConfiguration extends MessagingConfiguration {
    
    public static final String CONNECTION_FACTORY = "ConnectionFactory";
    public static final String DESTINATION = "Destination";
    public static final String CONFIG_ERROR_MSG = "Missing configuration item '%s'";
    
    public String getConnectionFactory() {
        return getProperty(CONNECTION_FACTORY);
    }

    public String getDestination() {
        return getProperty(DESTINATION);
    }
    
    
    public String getInitialContextFactory(){
        return getProperty(Context.INITIAL_CONTEXT_FACTORY);
    }
    
    public String getProviderURL(){
        return getProperty(Context.PROVIDER_URL);
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
            throw new XPathException(String.format(CONFIG_ERROR_MSG, CONNECTION_FACTORY));
        }
        
        String destination = getDestination();
        if(destination==null){
            throw new XPathException(String.format(CONFIG_ERROR_MSG, DESTINATION));
        }
        
    }
}
