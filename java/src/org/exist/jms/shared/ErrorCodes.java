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

import org.exist.dom.QName;
import org.exist.xquery.ErrorCodes.ErrorCode;

import static org.exist.jms.xquery.JmsModule.NAMESPACE_URI;
import static org.exist.jms.xquery.JmsModule.PREFIX;

/**
 * ErrorCodes definition
 */
public class ErrorCodes {

    public final static ErrorCode JMS000 = new JmsErrorCode("JMS000", "Generic exception.");
    public final static ErrorCode JMS001 = new JmsErrorCode("JMS001", "Internal IO error.");
    public final static ErrorCode JMS002 = new JmsErrorCode("JMS002", "Function does not exist.");
    public final static ErrorCode JMS003 = new JmsErrorCode("JMS003", "XML parse(r) error.");
    public final static ErrorCode JMS004 = new JmsErrorCode("JMS004", "Generic JMS Exception.");

    public final static ErrorCode JMS010 = new JmsErrorCode("JMS010", "Unauthorized usage.");
    public final static ErrorCode JMS011 = new JmsErrorCode("JMS011", "Configuration error.");

    public final static ErrorCode JMS020 = new JmsErrorCode("JMS020", "Receiver does not exist.");
    public final static ErrorCode JMS021 = new JmsErrorCode("JMS021", "Unsupported JMS message.");
    public final static ErrorCode JMS022 = new JmsErrorCode("JMS022", "Unable to convert JMS ObjectMessage to object.");
    public final static ErrorCode JMS023 = new JmsErrorCode("JMS023", "Received XML document is not valid.");

    public final static ErrorCode JMS025 = new JmsErrorCode("JMS025", "JMS connection is not initialized.");
    public final static ErrorCode JMS026 = new JmsErrorCode("JMS026", "No ConnectionFactory.");
    public final static ErrorCode JMS027 = new JmsErrorCode("JMS027", "Unable to convert object to JMS ObjectMessage.");

    public final static ErrorCode JMS030 = new JmsErrorCode("JMS030", "Missing collection trigger configuration.");
    public final static ErrorCode JMS031 = new JmsErrorCode("JMS031", "Missing resource.");


    protected final static class JmsErrorCode extends ErrorCode {

        public JmsErrorCode(String code, String description) {
            super(new QName(code, NAMESPACE_URI, PREFIX), description);
        }

    }

}
