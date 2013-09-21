/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2012 The eXist Project
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
 *
 *  $Id$
 */
package org.exist.messaging.xquery;

import org.exist.dom.QName;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

/**
 * @author wessels
 */


public class RegisterReceiver extends BasicFunction {
    
 public final static FunctionSignature signatures[] = {

        new FunctionSignature(
            new QName("register", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
            "Register function to receive JMS messages.",
            new SequenceType[]{
                new FunctionParameterSequenceType("callback", Type.FUNCTION_REFERENCE, Cardinality.ZERO_OR_ONE, "Function called when a JMS message is received"),
                new FunctionParameterSequenceType("config", Type.MAP, Cardinality.EXACTLY_ONE, "JMS configuration"),
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.ZERO_OR_ONE, "Confirmation message, if present")
        ),

        
    };

    public RegisterReceiver(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        
            throw new XPathException("Not implemented yet");

    }
    
}
