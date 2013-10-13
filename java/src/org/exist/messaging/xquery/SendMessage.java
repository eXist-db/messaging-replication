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
package org.exist.messaging.xquery;

import org.exist.dom.QName;
import org.exist.messaging.send.Sender;
import org.exist.messaging.configuration.JmsConfiguration;
import org.exist.messaging.configuration.JmsMessageProperties;
import org.exist.xquery.*;
import org.exist.xquery.functions.map.AbstractMapType;
import org.exist.xquery.value.*;

/**
 *  Implementation of the jms:send() function.
 * 
 * @author Dannes Wessels
 */

public class SendMessage extends BasicFunction {
    
    public final static FunctionSignature signatures[] = {

        new FunctionSignature(
            new QName("send", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
            "Send JMS message",
            new SequenceType[]{
                new FunctionParameterSequenceType("content", Type.ITEM, Cardinality.ONE, "Send message to remote server"),
                new FunctionParameterSequenceType("properties", Type.MAP,Cardinality.ONE_OR_MORE, "Application-defined property values"),
                new FunctionParameterSequenceType("config", Type.MAP, Cardinality.ONE, "JMS configuration")      
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.ONE, "Confirmation message")
        ),
        
    };

    public SendMessage(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        // Get content
        Item content = args[0].itemAt(0);

        // Get application properties
        AbstractMapType msgPropertiesMap = (AbstractMapType) args[1].itemAt(0);
        JmsMessageProperties messageProperties = new JmsMessageProperties();
        messageProperties.loadConfiguration(msgPropertiesMap);

        // Get JMS configuration
        AbstractMapType jmsConfigurationMap = (AbstractMapType) args[2].itemAt(0);
        JmsConfiguration jmsConfiguration = new JmsConfiguration();
        jmsConfiguration.loadConfiguration(jmsConfigurationMap);

        try {
            // Send message and return results
            Sender sender = new Sender(context);
            return sender.send(jmsConfiguration, messageProperties, content);

        } catch (XPathException ex) {
            LOG.error(ex.getMessage());
            ex.setLocation(this.line, this.column, this.getSource());
            throw ex;
        }   
    }    
}
