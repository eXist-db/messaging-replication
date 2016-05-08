/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2014 The eXist Project
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
package org.exist.jms.xquery;


import org.exist.jms.xquery.replication.RegisterReceiver;

import java.util.List;
import java.util.Map;

import org.exist.dom.QName;
import org.exist.jms.xquery.replication.SyncResource;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;

/**
 * JMS replication module
 *
 * @author Dannes Wessels
 */
public class ReplicationModule extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/xquery/replication";
    public final static String PREFIX = "replication";
    public final static String INCLUSION_DATE = "2014-04-10";
    public final static String RELEASED_IN_VERSION = "eXist-2.2";

    public final static FunctionDef[] functions = {
            new FunctionDef(RegisterReceiver.signatures[0], RegisterReceiver.class),
            new FunctionDef(SyncResource.signatures[0], SyncResource.class),
            new FunctionDef(SyncResource.signatures[1], SyncResource.class),

    };

    public final static QName EXCEPTION_QNAME =
            new QName("exception", ReplicationModule.NAMESPACE_URI, ReplicationModule.PREFIX);

    public final static QName EXCEPTION_MESSAGE_QNAME =
            new QName("exception-message", ReplicationModule.NAMESPACE_URI, ReplicationModule.PREFIX);

    public ReplicationModule(final Map<String, List<?>> parameters) throws XPathException {
        super(functions, parameters);
    }

    @Override
    public String getNamespaceURI() {
        return NAMESPACE_URI;
    }

    @Override
    public String getDefaultPrefix() {
        return PREFIX;
    }

    @Override
    public String getDescription() {
        return "JMS replication module";
    }

    @Override
    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }
}
