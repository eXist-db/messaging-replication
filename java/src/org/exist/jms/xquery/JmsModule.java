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
package org.exist.jms.xquery;

import java.util.List;
import java.util.Map;
import org.exist.dom.QName;
import org.exist.jms.xquery.management.ListReceivers;
import org.exist.jms.xquery.management.ManageReceivers;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;

/**
 * JMS module
 *
 * @author Dannes Wessels
 */
public class JmsModule extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/xquery/jms";
    public final static String PREFIX = "jms";
    public final static String INCLUSION_DATE = "2013-11-01";
    public final static String RELEASED_IN_VERSION = "eXist-2.2";
    
    public final static FunctionDef[] functions = {       
        new FunctionDef(ListReceivers.signatures[0], ListReceivers.class),

        new FunctionDef(ManageReceivers.signatures[0], ManageReceivers.class),
        new FunctionDef(ManageReceivers.signatures[1], ManageReceivers.class),
        new FunctionDef(ManageReceivers.signatures[2], ManageReceivers.class),
        new FunctionDef(ManageReceivers.signatures[3], ManageReceivers.class),
    };
    
    public final static QName EXCEPTION_QNAME =
            new QName("exception", JmsModule.NAMESPACE_URI, JmsModule.PREFIX);
    
    public final static QName EXCEPTION_MESSAGE_QNAME =
            new QName("exception-message", JmsModule.NAMESPACE_URI, JmsModule.PREFIX);

    public JmsModule(final Map<String, List<?>> parameters) throws XPathException {
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
        return "JMS messaging module";
    }

    @Override
    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }
}
