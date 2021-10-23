/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2016 The eXist Project
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
package org.exist.jms.xquery.replication;


import org.exist.dom.QName;
import org.exist.jms.replication.shared.ReplicationGuard;
import org.exist.jms.shared.Constants;
import org.exist.jms.shared.ErrorCodes;
import org.exist.jms.xquery.ReplicationModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import static org.exist.jms.shared.ErrorCodes.JMS010;

/**
 * Implementation of the replication:enable-trigger() function.
 *
 * @author Dannes Wessels
 */
public class ReplicationSwitch extends BasicFunction {

    public final static FunctionSignature[] signatures = {
            new FunctionSignature(
                    new QName("enable-trigger", ReplicationModule.NAMESPACE_URI, ReplicationModule.PREFIX),
                    "Globally switch on/off the replication trigger", new SequenceType[]{
                    new FunctionParameterSequenceType("newStatus", Type.BOOLEAN, Cardinality.EXACTLY_ONE, "Value true() enables replication."),},
                    new FunctionReturnSequenceType(Type.EMPTY, Cardinality.EMPTY_SEQUENCE, "")
            ),};

    public ReplicationSwitch(final XQueryContext context, final FunctionSignature signature) {
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

        try {
            final ReplicationGuard rg = ReplicationGuard.getInstance();

            final boolean newStatus = args[0].itemAt(0).toJavaObject(Boolean.class);

            rg.setReplicationEnabled(newStatus);

            return Sequence.EMPTY_SEQUENCE;


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
