/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2017 The eXist Project
 * http://exist-db.org
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.jms.shared.receive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class for managing Receivers
 *
 * @author Dannes Wessels
 */
public class ReceiversManager {

    private final static Logger LOG = LogManager.getLogger(ReceiversManager.class);
    private static ReceiversManager instance;
    private final Map<Integer, Receiver> receivers = new HashMap<>();

    private ReceiversManager() {
        // Nop
    }

    public synchronized static ReceiversManager getInstance() {

        if (null == instance) {
            LOG.debug("Initializing JMS receiver management");
            instance = new ReceiversManager();
        }

        return instance;
    }

    /**
     * Register receiver, a new unique ID is assigned..
     *
     * @param receiver The receiver class
     * @throws IllegalArgumentException When the argument has value NULL.
     */
    public void register(final Receiver receiver) {

        if (receiver == null) {
            throw new IllegalArgumentException("Receiver should not be null");
        }

        LOG.info(String.format("Registering receiver %s", receiver.getReceiverId()));
        receivers.put(receiver.getReceiverId(), receiver);
    }

    /**
     * Remove receiver by id.
     *
     * @param id Identification of receiver
     */
    public void remove(final Integer id) {
        LOG.info(String.format("Remove receiver %s", id));
        receivers.remove(id);
    }

    /**
     * Retrieve receiver by id.
     *
     * @param id Identification of receiver
     * @return The receiver
     */
    public Receiver get(final Integer id) {
        return receivers.get(id);
    }

    /**
     * Get receiver identifiers.
     *
     * @return Receiver IDs.
     */
    public Set<Integer> getIds() {
        return receivers.keySet();
    }

}
