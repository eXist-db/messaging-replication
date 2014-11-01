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
package org.exist.jms.xquery.messaging;

import org.exist.dom.QName;
import org.exist.jms.shared.Receiver;
import org.exist.jms.shared.ReceiversManager;
import org.exist.jms.shared.Constants;
import org.exist.jms.xquery.MessagingModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

/**
 * Implementation of the start-stop-close-getReport functions
 *
 * @author Dannes Wessels
 */
public class ManageReceivers extends BasicFunction {

    public static final String ID = "id";
    public static final String RECEIVER_ID = "Receiver ID";
    public static final String START = "start";
    public static final String STOP = "stop";
    public static final String CLOSE = "close";
    public static final String REPORT = "report";

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(START, MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX), "Start receiver",
        new SequenceType[]{
            new FunctionParameterSequenceType(ID, Type.INTEGER, Cardinality.EXACTLY_ONE, RECEIVER_ID),},
        new SequenceType(Type.ITEM, Cardinality.EMPTY)
        ),
        new FunctionSignature(
        new QName(STOP, MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX), "Stop receiver",
        new SequenceType[]{
            new FunctionParameterSequenceType(ID, Type.INTEGER, Cardinality.EXACTLY_ONE, RECEIVER_ID),},
        new SequenceType(Type.ITEM, Cardinality.EMPTY)
        ),
        new FunctionSignature(
        new QName(CLOSE, MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX), "Close receiver",
        new SequenceType[]{
            new FunctionParameterSequenceType(ID, Type.INTEGER, Cardinality.EXACTLY_ONE, RECEIVER_ID),},
        new SequenceType(Type.ITEM, Cardinality.EMPTY)
        ),
        new FunctionSignature(
        new QName(REPORT, MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX), "Get details of receiver",
        new SequenceType[]{
            new FunctionParameterSequenceType(ID, Type.INTEGER, Cardinality.EXACTLY_ONE, RECEIVER_ID),},
        new FunctionReturnSequenceType(Type.NODE, Cardinality.ONE, "XML fragment with receiver information")
        ),
        new FunctionSignature(
        new QName("list", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX),
        "Retrieve sequence of receiver IDs",
        new SequenceType[]{ // no params              
        },
        new FunctionReturnSequenceType(Type.INTEGER, Cardinality.ZERO_OR_MORE, "Sequence of receiver IDs")
        ),};

    public ManageReceivers(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the JMS group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.JMS_GROUP)) {
            String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.JMS_GROUP);
            XPathException ex = new XPathException(this, txt);
            LOG.error(txt, ex);
            throw ex;
        }
        
        ReceiversManager manager = ReceiversManager.getInstance();

        // Get receiver by ID
        Integer id = args[0].toJavaObject(Integer.class);
        Receiver receiver = manager.get(id);

        // Verify if receiver is available
        if (receiver == null) {
            throw new XPathException(this, String.format("No receiver exists for id '%s'", id));
        }

        try {
            // Holder for return values
            Sequence returnValue = Sequence.EMPTY_SEQUENCE;

            if (isCalledAs(START)) {
                // Start receiver
                receiver.start();

            } else if (isCalledAs(STOP)) {
                // Stop receiver
                receiver.stop();

            } else if (isCalledAs(CLOSE)) {
                // Close and remove receiver
                try {
                    receiver.close();
                } finally {
                    manager.remove(id);
                }

            } else if (isCalledAs(REPORT)) {
                // Return report
                returnValue = receiver.getReport();

            } else {
                throw new XPathException(this, String.format("Function '%s' does not exist.", getSignature().getName().getLocalName()));
            }

            return returnValue;

        } catch (XPathException ex) {
            LOG.error(ex.getMessage());
            ex.setLocation(this.line, this.column, this.getSource());
            throw ex;

        } catch (Throwable t) {
            LOG.error(t.getMessage());
            XPathException ex = new XPathException(this, t);
            throw ex;
        }
    }
}
