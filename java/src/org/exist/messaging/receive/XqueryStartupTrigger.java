package org.exist.messaging.receive;

import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.exist.replication.shared.ClientParameters;
import org.exist.security.xacml.AccessContext;
import org.exist.source.Source;
import org.exist.source.SourceFactory;
import org.exist.storage.DBBroker;
import org.exist.storage.StartupTrigger;
import org.exist.xquery.CompiledXQuery;
import org.exist.xquery.XQuery;
import org.exist.xquery.XQueryContext;

/**
 * Startup Trigger to fire a Xquery function that starts a receiver.
 *
 * @author Dannes Wessels
 */
public class XqueryStartupTrigger implements StartupTrigger {

    protected final static Logger LOG = Logger.getLogger(XqueryStartupTrigger.class);

    @Override
    public void execute(DBBroker broker, Map<String, List<? extends Object>> params) {
        
        LOG.info("Starting XQuery Startup Trigger");

        try {
            XQuery service = broker.getXQueryService();
            final XQueryContext context = service.newContext(AccessContext.TRIGGER);

            Source source = SourceFactory.getSource(broker, null, "xmldb:exist:///db/run.xq", false);
            
            if (source == null) {
                LOG.error("No xquery found");
                
            } else {

                CompiledXQuery compiledQuery = service.compile(context, source);

                service.execute(compiledQuery, null, null);

                context.runCleanupTasks();
            }

        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
        } finally {
            LOG.info("Finished");
        }

    }

}
