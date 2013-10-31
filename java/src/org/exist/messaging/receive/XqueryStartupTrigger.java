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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import org.exist.security.xacml.AccessContext;
import org.exist.source.Source;
import org.exist.source.SourceFactory;
import org.exist.storage.DBBroker;
import org.exist.storage.StartupTrigger;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.CompiledXQuery;
import org.exist.xquery.XQuery;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.Sequence;

/**
 * Startup Trigger to fire Xquery scripts during database startup.
 *
 * @author Dannes Wessels
 */
public class XqueryStartupTrigger implements StartupTrigger {

    protected final static Logger LOG = Logger.getLogger(XqueryStartupTrigger.class);

    @Override
    public void execute(DBBroker broker, Map<String, List<? extends Object>> params) {
        
        LOG.info("Starting Startup Trigger for stored XQueries");

        for (String path : getParameters(params)) {
            executeQuery(broker, path);
        }

    }

    /**
     * Get all XQuery paths
     */
    private List<String> getParameters(Map<String, List<? extends Object>> params) {

        // Return values
        List<String> paths = new ArrayList<String>();

        // The complete data map
        Set<Map.Entry<String, List<? extends Object>>> data = params.entrySet();

        // Iterate over all entries
        for (Map.Entry<String, List<? extends Object>> entry : data) {

            // only the 'xpath' parameter is used.
            if ("xquery".equals(entry.getKey())) {

                // Iterate over all values (object lists)
                List<? extends Object> list = entry.getValue();
                for (Object o : list) {
                    
                    if (o instanceof String) {
                        String value = (String) o;

                        if (value.startsWith("/")) {

                            // Rewrite to URL in database
                            value = XmldbURI.EMBEDDED_SERVER_URI_PREFIX + value;

                            // Prevent double entries
                            if (!paths.contains(value)) {
                                paths.add(value);
                            }

                        } else {
                            LOG.error(String.format("Path '%s' should start with a '/'", value));
                        }
                    }
                }
            }

        }

        LOG.debug(String.format("Found %s 'xquery' entries.", paths.size()));

        return paths;
    }

    /**
     * Execute xquery on path
     *
     * @param broker eXist database broker
     * @param path path to query, gromatted as xmldb:exist:///db///
     */
    private void executeQuery(DBBroker broker, String path) {
        XQueryContext context = null;
        try {
            XQuery service = broker.getXQueryService();
            context = service.newContext(AccessContext.TRIGGER);

            // Get path to xquery
            Source source = SourceFactory.getSource(broker, null, path, false);

            if (source == null) {
                LOG.info(String.format("No Xquery found at '%s'", path));

            } else {
                // Compile query
                CompiledXQuery compiledQuery = service.compile(context, source);

                LOG.info(String.format("Starting Xquery at '%s'", path));

                // Execute
                Sequence result = service.execute(compiledQuery, null);

                // Log results
                LOG.info(String.format("Result xquery: '%s'", result.getStringValue()));

            }

        } catch (Throwable t) {
            // Dirty, catch it all
            LOG.error(String.format("An error occured during preparation/execution of the xquery script %s: %s", path, t.getMessage()), t);

        } finally {
            if (context != null) {
                context.runCleanupTasks();
            }
        }
    }

}
