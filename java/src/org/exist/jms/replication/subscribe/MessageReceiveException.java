/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2012 The eXist Project
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
 *
 *  $Id$
 */
package org.exist.jms.replication.subscribe;

import org.exist.jms.shared.eXistMessage;

/**
 * Class for reporting problems during handling received messages. Must
 * be a runtime exception.
 *
 * @author Dannes Wessels
 */
public class MessageReceiveException extends RuntimeException {

    private eXistMessage message;

    public MessageReceiveException() {
        super();
    }

    public MessageReceiveException(final String msg) {
        super(msg);
    }

    public MessageReceiveException(final String msg, final eXistMessage message) {
        super(msg);
        this.message = message;
        this.message.resetPayload();
    }

    public MessageReceiveException(final Throwable t) {
        super(t);
    }

    public MessageReceiveException(final String msg, final Throwable t) {
        super(msg, t);
    }


    public MessageReceiveException(final String msg, final Throwable t, final eXistMessage message) {
        super(msg, t);
        this.message = message;
        this.message.resetPayload();
    }

    public eXistMessage getExistMessage() {
        return message;
    }

    public void setExistMessage(eXistMessage message) {
        this.message = message;
        this.message.resetPayload();
    }
}
