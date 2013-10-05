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

import java.util.List;
import java.util.Map;
import org.exist.dom.QName;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;

/**
 * JMS module
 *
 * @author Dannes Wessels
 */
public class MessagingModule extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/xquery/messaging";
    public final static String PREFIX = "jms";
    public final static String INCLUSION_DATE = "2013-11-01";
    public final static String RELEASED_IN_VERSION = "eXist-2.2";
    public final static FunctionDef[] functions = {
        new FunctionDef(SendMessage.signatures[0], SendMessage.class),
        new FunctionDef(RegisterReceiver.signatures[0], RegisterReceiver.class),
        new FunctionDef(CloseReceiver.signatures[0], CloseReceiver.class),
        new FunctionDef(ListReceivers.signatures[0], ListReceivers.class),
        new FunctionDef(StartReceiver.signatures[0], StartReceiver.class),
        new FunctionDef(StopReceiver.signatures[0], StopReceiver.class),
        new FunctionDef(InfoReceiver.signatures[0], InfoReceiver.class),
    };
    public final static QName EXCEPTION_QNAME =
            new QName("exception", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX);
    public final static QName EXCEPTION_MESSAGE_QNAME =
            new QName("exception-message", MessagingModule.NAMESPACE_URI, MessagingModule.PREFIX);

    public MessagingModule(Map<String, List<? extends Object>> parameters) throws XPathException {
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
        return "A module for sending JMS messages";
    }

    @Override
    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }
}
