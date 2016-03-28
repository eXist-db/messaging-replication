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
package org.exist.jms.xquery.management;

import org.exist.dom.QName;
import org.exist.jms.shared.ReceiversManager;
import org.exist.jms.shared.Constants;
import org.exist.jms.xquery.JmsModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.*;

/**
 *  Implementation of the jms:list() function. Provides information about the receivers.
 * 
 * @author Dannes Wessels
 */
public class ListReceivers extends BasicFunction {
    
 public final static FunctionSignature signatures[] = {

        new FunctionSignature(
            new QName("list", JmsModule.NAMESPACE_URI, JmsModule.PREFIX),
            "Retrieve sequence of reciever IDs",
            new SequenceType[]{
                          // no params              
            },
            new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "Sequence of receiver IDs")
        ),
        
    };

    public ListReceivers(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the JMS group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.JMS_GROUP)) {
            final String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.JMS_GROUP);
            final XPathException ex = new XPathException(this, txt);
            LOG.error(txt);
            throw ex;
        }

        // Get object that manages the receivers
        final ReceiversManager manager = ReceiversManager.getInstance();

        // Conten holfer results
        final ValueSequence returnSequence = new ValueSequence();

        // Collect IDs
        manager.getIds().stream().forEach((id) -> returnSequence.add(new IntegerValue(id)));

        // Return IDs
        return returnSequence;
    }
    
}
