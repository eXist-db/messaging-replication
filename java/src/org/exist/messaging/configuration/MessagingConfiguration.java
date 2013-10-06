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

import java.math.BigInteger;
import java.util.Properties;

import org.apache.log4j.Logger;

import org.exist.xquery.XPathException;
import org.exist.xquery.functions.map.AbstractMapType;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.BooleanValue;
import org.exist.xquery.value.DoubleValue;
import org.exist.xquery.value.FloatValue;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.ValueSequence;

/**
 *  Wrapper for properties object.
 * 
 * @author Dannes Wessels
 */
public class MessagingConfiguration extends Properties {
    
    private final static Logger LOG = Logger.getLogger(MessagingConfiguration.class);
    
   /**
     * Load data from XQuery map-type.
     *
     * @param map The XQuery map
     * @return The converted map
     * @throws XPathException Something bad happened.
     */
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

            // Parse data only if the key is a String
            if (values instanceof StringValue) {
                StringValue singlevalue = (StringValue) values;
                setProperty(keyValue, singlevalue.getStringValue());

            } else if (values instanceof IntegerValue) {
                IntegerValue singleValue = (IntegerValue) values;
                put(keyValue, singleValue.toJavaObject(BigInteger.class));

            } else if (values instanceof DoubleValue) {
                DoubleValue singleValue = (DoubleValue) values;
                put(keyValue, singleValue.toJavaObject(Double.class));
                
            } else if (values instanceof BooleanValue) {
                BooleanValue singleValue = (BooleanValue) values;
                put(keyValue, singleValue.toJavaObject(Boolean.class));
                
            } else if (values instanceof FloatValue) {
                FloatValue singleValue = (FloatValue) values;
                put(keyValue, singleValue.toJavaObject(Float.class));
                
            } else if (values instanceof ValueSequence) {
                LOG.info(String.format("Cannot convert a sequence of values for key '%s' into JMS message properties", keyValue));
                
            } else {
                LOG.info(String.format("Cannot convert map entry '%s'/'%s' into a JMS message property", keyValue, values.getStringValue()));
            }

        }
    }
}
