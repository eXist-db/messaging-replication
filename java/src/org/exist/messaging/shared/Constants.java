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
package org.exist.messaging.shared;

/**
 * Shared constants
 *
 * @author Dannes Wessels
 */
public class Constants {

    /*
     * eXist-db knows about XML and NON-XML data
     */
    public static final String DATA_TYPE_BINARY = "binary";
    public static final String DATA_TYPE_XML = "xml";
    /*
     * eXist-db internal nata
     */
    public static final String EXIST_DATA_TYPE = "exist.data.type";
    public static final String EXIST_DOCUMENT_MIMETYPE = "exist.document.mimetype";
    public static final String EXIST_DOCUMENT_URI = "exist.document.uri";
    public static final String EXIST_XPATH_DATATYPE = "exist.xpath.datatype";
    /*
     * eXist-db JMS instance id
     */
    public static final String EXIST_INSTANCE_ID = "exist.instance-id";
    /*
     * JMS reporting
     */
    public static final String JMS = "jms";
    public static final String JMS_CORRELATION_ID = "jms.correlation-id";
    public static final String JMS_EXPIRATION = "jms.expiration";
    public static final String JMS_MESSAGE_ID = "jms.message-id";
    public static final String JMS_PRIORITY = "jms.priority";
    public static final String JMS_TIMESTAMP = "jms.timestamp";
    public static final String JMS_TYPE = "jms.type";
    /*
     * Parameter names for setting-up a connection
     */
    public static final String CONNECTION_FACTORY = "connection-factory";
    public static final String DESTINATION = "destination";
    /*
     * Connection parameters
     */
    public static final String JMS_CONNECTION_USERNAME = "connection.username";
    public static final String JMS_CONNECTION_PASSWORD = "connection.password";
    public static final String CLIENT_ID = "connection.client-id";
    /**
     * JMS message selector
     */
    public static final String MESSAGE_SELECTOR = "consumer.message-selector";
    /*
     * Topic parameters (durable, prefent backfire)
     */
    public static final String DURABLE = "subscriber.durable";
    public static final String NO_LOCAL = "subscriber.nolocal";
    public static final String SUBSCRIBER_NAME = "subscriber.name";
}
