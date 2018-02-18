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
import org.exist.jms.messaging.MessagingJmsListener;
import org.exist.jms.shared.Constants;
import org.exist.jms.shared.ErrorCodes;
import org.exist.jms.shared.JmsConfiguration;
import org.exist.jms.shared.receive.Receiver;
import org.exist.jms.shared.receive.ReceiversManager;
import org.exist.jms.xquery.MessagingModule;
import org.exist.storage.DBBroker;
import org.exist.xquery.*;
import org.exist.xquery.functions.map.AbstractMapType;
import org.exist.xquery.functions.request.RequestModule;
import org.exist.xquery.value.*;

import static org.exist.jms.shared.ErrorCodes.JMS010;

/**
 * Implementation of the jms:register() function.
 *
 * @author Dannes Wessels
 */
public class RegisterReceiver extends BasicFunction {


    public final static FunctionSignature signatures[] = {

            new FunctionSignature(
                    new QName("register", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
                    "Register function to receive JMS messages.",
                    new SequenceType[]{
                            new FunctionParameterSequenceType("callback", Type.FUNCTION_REFERENCE, Cardinality.EXACTLY_ONE, "Function called when a JMS message is received"),
                            new FunctionParameterSequenceType("parameters", Type.ITEM, Cardinality.ZERO_OR_MORE, "Additional function parameters"),
                            new FunctionParameterSequenceType("jmsConfiguration", Type.MAP, Cardinality.EXACTLY_ONE, "JMS configuration"),},
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "Receiver ID")
            ),

    };

    public RegisterReceiver(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the JMS group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.JMS_GROUP)) {
            final String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.JMS_GROUP);
            final XPathException ex = new XPathException(this, JMS010, txt);
            LOG.error(txt, ex);
            throw ex;
        }

        try(DBBroker broker = context.getBroker()){
            // Get object that manages the receivers
            final ReceiversManager manager = ReceiversManager.getInstance();

            // Get function
            final FunctionReference reference = (FunctionReference) args[0].itemAt(0);

            // Get optional function parameters
            final Sequence functionParams = args[1];

            // Get JMS configuration
            final AbstractMapType configMap = (AbstractMapType) args[2].itemAt(0);
            final JmsConfiguration config = new JmsConfiguration();
            config.loadConfiguration(configMap);

            // Remove Request module from expression and xquery context
            this.getContext().setModule(RequestModule.NAMESPACE_URI, null);
            context.setModule(RequestModule.NAMESPACE_URI, null);

            // Setup listener, pass correct User object
            // get user via Broker for compatibility < existdb 2.2
            final MessagingJmsListener myListener = new MessagingJmsListener(broker.getCurrentSubject(), reference, functionParams, context);

            // Create receiver
            final Receiver receiver = new Receiver(config, myListener); // TODO check use .copyContext() ?

            // Register, initialize and start receiver
            manager.register(receiver);
            receiver.initialize();
            receiver.start();

            // Return identification
            return new IntegerValue(receiver.getReceiverId());

        } catch (final XPathException ex) {
            LOG.error(ex.getMessage());
            ex.setLocation(this.line, this.column, this.getSource());
            throw ex;

        } catch (final Throwable t) {
            LOG.error(t.getMessage(), t);
            throw new XPathException(this, ErrorCodes.JMS000, t);
        }
    }
}
