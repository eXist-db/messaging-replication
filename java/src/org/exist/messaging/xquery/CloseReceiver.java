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
 * Implementation of the jms:close() function. Releases all resources to the system.
 * 
 * @author Dannes Wessels
 */
public class CloseReceiver extends BasicFunction {
    
 public final static FunctionSignature signatures[] = {

        new FunctionSignature(
            new QName("close", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
            "Delete receiver",
            new SequenceType[]{
                  new FunctionParameterSequenceType("id", Type.STRING, Cardinality.EXACTLY_ONE, "Receiver ID"),            
           
            },
            new FunctionReturnSequenceType(Type.EMPTY, Cardinality.ONE, "XML fragment with receiver information")
        ),

        
    };

    public CloseReceiver(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
    
        ReceiversManager manager = ReceiversManager.getInstance();
        
        String id = args[0].getStringValue();
        
        Receiver receiver = manager.get(id);
        receiver.close();
        
        manager.remove(id);
        
        return Sequence.EMPTY_SEQUENCE;
    
    }
    
}
