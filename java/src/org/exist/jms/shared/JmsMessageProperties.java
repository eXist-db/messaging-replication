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

import java.util.Properties;
import org.exist.jms.replication.shared.ClientParameters;

/**
 *  Class for containing JMS Message properties (application specific)
 * 
 * @author Dannes Wessels
 */
public class JmsMessageProperties extends MessagingConfiguration {

    public void loadParameters(final ClientParameters params) {

        // Get properties
        final Properties props = params.getProps();

        // TODO remove properties needed for connection
        //props.remove(LOG)

        // Use it all
        this.putAll(props);
    }
}
