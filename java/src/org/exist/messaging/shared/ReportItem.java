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
package org.exist.messaging.shared;

import java.util.Date;
import javax.jms.JMSException;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.exist.dom.QName;
import org.exist.memtree.MemTreeBuilder;

/**
 * Container for reporting a problem with some meta data.
 *
 * @author Dannes Wessels
 */
public class ReportItem {

    public enum CONTEXT {
        RECEIVER, LISTENER, CONNECTION, NOTDEFINED
    };


    public ReportItem(Throwable throwable, CONTEXT context) {
        this.timestamp = new Date();
        this.throwable = throwable;
        this.context = context;
    }

    private Throwable throwable = new Throwable("EMPTY");
    private CONTEXT context = CONTEXT.NOTDEFINED;

    private Date timestamp;

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public Throwable getThowable() {
        return throwable;
    }

    public String getTimeStamp() {
        return DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(timestamp);
    }

    public String getMessage() {
        return throwable.getMessage();
    }

    public String getContextName() {
        return context.toString().toLowerCase();
    }

    public void writeError(MemTreeBuilder builder) {
        builder.startElement("", "error", "error", null);
        builder.addAttribute(new QName("src", null, null), getContextName());
        builder.addAttribute(new QName("timestamp", null, null), getTimeStamp());
        builder.addAttribute(new QName("exception", null, null), getThowable().getClass().getSimpleName());

        String msg = getMessage();
        if (getThowable() instanceof JMSException) {

            JMSException jmse = (JMSException) getThowable();
            if (jmse.getErrorCode() != null) {
                msg = msg + " (code=" + jmse.getErrorCode() + ")";
            }

        }

        builder.characters(msg);
        builder.endElement();
    }
}
