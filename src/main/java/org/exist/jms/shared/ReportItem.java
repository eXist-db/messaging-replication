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
package org.exist.jms.shared;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.exist.dom.QName;
import org.exist.dom.memtree.MemTreeBuilder;
import org.exist.jms.replication.subscribe.MessageReceiveException;

import javax.jms.JMSException;
import java.util.Date;
import java.util.Locale;

/**
 * Container for reporting a problem with some meta data.
 *
 * @author Dannes Wessels
 */
public class ReportItem {

    private final Date timestamp;
    private Throwable throwable = new Throwable("EMPTY");
    private CONTEXT context = CONTEXT.NOTDEFINED;

    public ReportItem(final Throwable throwable, final CONTEXT context) {
        this.timestamp = new Date();
        this.throwable = throwable;
        this.context = context;
    }

    public void setThrowable(final Throwable throwable) {
        this.throwable = throwable;
    }

    public Throwable getThowable() {
        return throwable;
    }

    public String getTimeStamp() {
        return DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.format(timestamp);
    }

    public String getMessage() {
        return throwable.getMessage();
    }

    public String getContextName() {
        return context.toString().toLowerCase(Locale.US);
    }

    public void writeError(final MemTreeBuilder builder) {
        final Throwable t = getThowable();

        builder.startElement("", "error", "error", null);
        builder.addAttribute(new QName("src", null, null), getContextName());
        builder.addAttribute(new QName("timestamp", null, null), getTimeStamp());
        builder.addAttribute(new QName("exception", null, null), t.getClass().getSimpleName());

        String msg = getMessage();

        // Treat JMSException a bit different
        if (t instanceof JMSException) {
            final JMSException jmse = (JMSException) t;
            if (jmse.getErrorCode() != null) {
                msg += (" (code=" + jmse.getErrorCode() + ")");
            }
        }

        // Treat MessageReceiveException a bit different too
        if (t instanceof MessageReceiveException) {
            final MessageReceiveException mre = (MessageReceiveException) t;
            msg += " (" + mre.getExistMessage().getReport() + ")";
        }

        builder.characters(msg);
        builder.endElement();
    }

    public enum CONTEXT {
        RECEIVER, LISTENER, CONNECTION, NOTDEFINED
    }
}
