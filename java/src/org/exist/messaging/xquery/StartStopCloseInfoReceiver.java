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
import org.exist.messaging.receive.Receiver;
import org.exist.messaging.receive.ReceiversManager;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

/**
 * Implementation of the start-stop-close-info functions
 *
 * @author Dannes Wessels
 */
public class StartStopCloseInfoReceiver extends BasicFunction {

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
                new QName("start", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
                "Start receiver",
                new SequenceType[]{
                    new FunctionParameterSequenceType("id", Type.STRING, Cardinality.EXACTLY_ONE, "Receiver ID"),
                },
                new SequenceType(Type.ITEM, Cardinality.EMPTY)
            ),
        
        new FunctionSignature(
                new QName("stop", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
                "Stop receiver",
                new SequenceType[]{
                    new FunctionParameterSequenceType("id", Type.STRING, Cardinality.EXACTLY_ONE, "Receiver ID"),
                },
                new SequenceType(Type.ITEM, Cardinality.EMPTY)
            ),
        
        new FunctionSignature(
                new QName("close", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
                "Close receiver",
                new SequenceType[]{
                    new FunctionParameterSequenceType("id", Type.STRING, Cardinality.EXACTLY_ONE, "Receiver ID"),
                },
                new SequenceType(Type.ITEM, Cardinality.EMPTY)
            ),
        
        new FunctionSignature(
            new QName("info", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
            "Get details of receiver",
            new SequenceType[]{
                new FunctionParameterSequenceType("id", Type.STRING, Cardinality.EXACTLY_ONE, "Receiver ID"),
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.ONE, "XML fragment with receiver information")
            ),
        
       new FunctionSignature(
            new QName("list", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
            "Retrieve sequence of receiver IDs",
            new SequenceType[]{
                          // no params              
            },
            new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "Sequence of receiver IDs")
        ),
    };

    public StartStopCloseInfoReceiver(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        
        ReceiversManager manager = ReceiversManager.getInstance();

        // Get receiver by ID
        String id = args[0].getStringValue();
        Receiver receiver = manager.get(id);

        // Verify if receiver is available
        if (receiver == null) {
            throw new XPathException(String.format("No receiver exists for id '%s'", id));
        }
        
        // Holder for return values
        Sequence returnValue = Sequence.EMPTY_SEQUENCE;

        if (isCalledAs("start")) {
            // Start receiver
            receiver.start();

        } else if (isCalledAs("stop")) {
            // Stop receiver
            receiver.stop();

        } else if (isCalledAs("close")) {
            // Close and remove receiver 
            try {
                receiver.close();
            } finally {
                manager.remove(id);
            }
          
        } else if (isCalledAs("info")) { 
            // Return report
            returnValue = receiver.info(); 

        } else {
            throw new XPathException("Function '" + getSignature().getName().getLocalName() + "' does not exist.");
        }

        return returnValue;
    }
}
