/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2016 The eXist Project
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
package org.exist.jms.replication.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Helper class to detect if a resource should be replicated or not.
 */
public class ReplicationGuard {

    private final static Logger LOGGER = LogManager.getLogger(ReplicationGuard.class);
    private static volatile ReplicationGuard instance = null;
    boolean replicationEnabled = true;

    private ReplicationGuard() {
        // Empty
    }

    public static ReplicationGuard getInstance() {

        if (instance == null) {
            synchronized (ReplicationGuard.class) {
                // be sure
                if (instance == null) {
                    instance = new ReplicationGuard();
                }
            }
        }

        return instance;

    }

    public boolean getReplicationEnabled() {
        return this.replicationEnabled;
    }

    public void setReplicationEnabled(final boolean newStatus) {

        LOGGER.info("Replication is switched {}", newStatus ? "ON" : "OFF");

        this.replicationEnabled = newStatus;
    }
}
