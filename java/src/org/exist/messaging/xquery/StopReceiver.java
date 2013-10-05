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
 *  Implementation of the jms:stop() function. Stops a receiver.
 * 
 * @author Dannes Wessels
 */
public class StopReceiver extends BasicFunction {
    
 public final static FunctionSignature signatures[] = {

        new FunctionSignature(
            new QName("stop", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
            "Stop receiver",
            new SequenceType[]{
                  new FunctionParameterSequenceType("id", Type.STRING, Cardinality.EXACTLY_ONE, "Receiver ID"),            
           
            },
            new SequenceType(Type.ITEM, Cardinality.EMPTY)
        ),

        
    };

    public StopReceiver(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
    
        // Get receiver ID
        String id = args[0].getStringValue();
        
        // Obtain receiver
        ReceiversManager manager = ReceiversManager.getInstance();
        Receiver receiver = manager.get(id);
        
        // Verify if receiver is available
        if (receiver == null) {
            throw new XPathException(String.format("No receiver exists for id '%s'", id));
        }
        
        // Stop receiver
        receiver.stop();
        
        return Sequence.EMPTY_SEQUENCE;
    
    }
    
}
