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
import org.apache.commons.lang3.time.DateFormatUtils;

/**
 * Container for reporting a problem
 *
 * @author Dannes Wessels
 */
public class ReportItem {

    public ReportItem() {
        timestamp = new Date();
    }

    public ReportItem(Throwable throwable) {
        this();
        this.throwable = throwable;
    }

    private Throwable throwable = new Throwable("EMPTY");

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

}
