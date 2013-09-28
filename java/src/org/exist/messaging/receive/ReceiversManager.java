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
package org.exist.messaging.receive;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Class for managing Receivers
 *
 * @author Dannes Wessels
 */
public class ReceiversManager {

    //private static List<Receiver> receivers = new ArrayList<Receiver>();
    private Map<String, Receiver> receivers = new HashMap<String, Receiver>();
    private static ReceiversManager instance;

    private ReceiversManager() {
        // Nop
    }

    public static ReceiversManager getInstance() {

        if (null == instance) {
            instance = new ReceiversManager();
        }

        return instance;
    }

    public String add(Receiver receiver) {

        if (receiver == null) {
            throw new IllegalArgumentException("Receiver should not be null");
        }

        String id = UUID.randomUUID().toString();
        receivers.put(id, receiver);
        return id;
    }

    public void remove(String id) {
        receivers.remove(id);
    }

    public Receiver get(String id) {
        return receivers.get(id);
    }

    public Set<String> getIds() {
        return receivers.keySet();
    }

    public String list() {

        for (String key : receivers.keySet()) {
            Receiver receiver = receivers.get(key);

        }
        return null;
    }
}
