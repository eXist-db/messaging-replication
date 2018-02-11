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
package org.exist.jms.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.dom.memtree.DocumentBuilderReceiver;
import org.exist.dom.persistent.NodeHandle;
import org.exist.numbering.NodeId;
import org.exist.storage.DBBroker;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.Type;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.util.Arrays;
import java.util.Properties;

/**
 * Container wrapper for the eXistMessage object.
 *
 * @author Dannes Wessels
 */
public class eXistMessageItem implements Item {

    protected final static Logger LOG = LogManager.getLogger(eXistMessageItem.class);

    private eXistMessage data = null;

    public eXistMessage getData() {
        return data;
    }

    public void setData(final eXistMessage data) {
        this.data = data;
    }

    @Override
    public int getType() {
        return Type.ITEM;
    }

    @Override
    public String getStringValue() throws XPathException {
        if (data != null) {
            return Arrays.toString(data.getPayload());
        }
        return null;
    }

    @Override
    public Sequence toSequence() {
        throw new UnsupportedOperationException("Not supported yet. toSequence");
    }

    @Override
    public AtomicValue convertTo(final int requiredType) throws XPathException {
        throw new UnsupportedOperationException("Not supported yet. convertTo");
    }

    @Override
    public AtomicValue atomize() throws XPathException {
        throw new UnsupportedOperationException("Not supported yet. atomize");
    }

    @Override
    public void toSAX(final DBBroker broker, final ContentHandler handler, final Properties properties) throws SAXException {
        throw new UnsupportedOperationException("Not supported yet. toSAX");
    }

    @Override
    public void copyTo(final DBBroker broker, final DocumentBuilderReceiver receiver) throws SAXException {
        throw new UnsupportedOperationException("Not supported yet. copyTo");
    }

    @Override
    public int conversionPreference(final Class<?> javaClass) {
        throw new UnsupportedOperationException("Not supported yet. conversionPreference");
    }

    @Override
    public <T> T toJavaObject(final Class<T> target) throws XPathException {
        throw new UnsupportedOperationException("Not supported yet. toJavaObject");
    }

    @Override
    public void nodeMoved(final NodeId oldNodeId, final NodeHandle newNode) {
        throw new UnsupportedOperationException("Not supported yet. nodeMoved");
    }

    @Override
    public void destroy(final XQueryContext context, final Sequence contextSequence) {
        throw new UnsupportedOperationException("Not supported yet. destroy");
    }
}
