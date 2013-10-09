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

    public static final String DATA_TYPE_BINARY = "binary";
    public static final String DATA_TYPE_XML = "xml";
    
    public static final String EXIST_DATA_TYPE = "exist.data.type";
    public static final String EXIST_DOCUMENT_MIMETYPE = "exist.document.mimetype";
    public static final String EXIST_DOCUMENT_URI = "exist.document.uri";
    public static final String EXIST_XPATH_DATATYPE = "exist.xpath.datatype";
    
    public static final String EXIST_INSTANCE_ID = "exist.instance.id";
    
    public static final String JMS = "JMS";
    public static final String JMS_CORRELATION_ID = "JMSCorrelationID";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_MESSAGE_ID = "JMSMessageID";
    public static final String JMS_PRIORITY = "JMSPriority";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";
    public static final String JMS_TYPE = "JMSType";
    
    public static final String JMS_CONNECTION_USERNAME = "jms.connection.username";
    public static final String JMS_CONNECTION_PASSWORD = "jms.connection.password";
    
    public static final String CONNECTION_FACTORY = "ConnectionFactory";
    public static final String DESTINATION = "Destination";
    public static final String CLIENT_ID = "ClientID";
    public static final String MESSAGE_SELECTOR = "MessageSelector";
    
    public static final String DURABLE = "durable";
    public static final String NO_LOCAL = "nolocal";
    public static final String SUBSCRIBER_NAME = "subscribername";
}
