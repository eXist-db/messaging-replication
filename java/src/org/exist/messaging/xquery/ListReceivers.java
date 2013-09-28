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
import org.exist.messaging.receive.ReceiversManager;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

/**
 *  Implementation of the jms:list() function. Provides information about the receivers.
 * 
 * @author Dannes Wessels
 */
public class ListReceivers extends BasicFunction {
    
 public final static FunctionSignature signatures[] = {

        new FunctionSignature(
            new QName("list", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
            "Retrieve information about all rregistered receivers.",
            new SequenceType[]{
                              
           
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.ONE, "XML fragment with receiver information")
        ),

        
    };

    public ListReceivers(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
    
        ReceiversManager manager = ReceiversManager.getInstance();
        
        ValueSequence sequence = new ValueSequence();
        
        for(String id : manager.getIds()){
            sequence.add( new StringValue(id) );
        }
        
        return sequence;
    
    }
    
}
