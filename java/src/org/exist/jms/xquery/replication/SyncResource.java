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
package org.exist.jms.xquery.replication;


import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.collections.CollectionConfiguration;
import org.exist.collections.CollectionConfigurationException;
import org.exist.collections.CollectionConfigurationManager;
import org.exist.collections.triggers.DocumentTrigger;
import org.exist.collections.triggers.TriggerProxy;
import org.exist.dom.QName;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.jms.replication.publish.ReplicationTrigger;
import org.exist.jms.shared.Constants;
import org.exist.jms.xquery.ReplicationModule;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.storage.txn.TransactionManager;
import org.exist.storage.txn.Txn;
import org.exist.util.LockException;
import org.exist.util.ParametersExtractor;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.*;
import org.exist.xquery.value.*;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.List;
import java.util.Map;

import static org.exist.collections.CollectionConfiguration.NAMESPACE;

/**
 * Implementation of the replication:register() function.
 *
 * @author Dannes Wessels
 */
public class SyncResource extends BasicFunction {

    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName("sync", ReplicationModule.NAMESPACE_URI, ReplicationModule.PREFIX),
                    "Synchronize resource", new SequenceType[]{
                    new FunctionParameterSequenceType("path", Type.STRING, Cardinality.EXACTLY_ONE, "Path to resource"),},
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "Receiver ID")
            ),};

    public SyncResource(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the JMS group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.JMS_GROUP)) {
            final String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.JMS_GROUP);
            final XPathException ex = new XPathException(this, txt);
            LOG.error(txt, ex);
            throw ex;
        }

        try {

            // Get JMS configuration
            final String resource = (String) args[0].itemAt(0).getStringValue();


            // Determine
            final XmldbURI sourcePathURI = XmldbURI.create(resource);
            final XmldbURI parentCollectionURI = sourcePathURI.removeLastSegment();
            final XmldbURI resourceURI = sourcePathURI.lastSegment();

            ReplicationTrigger trigger = new ReplicationTrigger();

            final DBBroker broker = context.getBroker();
            final TransactionManager txnManager = broker.getBrokerPool().getTransactionManager();

            final Collection parentCollection = getCollection(broker, parentCollectionURI);


            // TODO
            final Map<String, List<?>> parameters = getParameters(broker, parentCollection);


            try (Txn txn = txnManager.beginTransaction()) {

                // Configure with collection that contains the resouce
                trigger.configure(broker, parentCollection, parameters);

                if (isCollection(broker, sourcePathURI)) {

                    // resource points to a collection
                    final Collection theCollection = getCollection(broker, sourcePathURI);
                    trigger.afterUpdateCollectionMetadata(broker, txn, theCollection);
                    theCollection.release(Lock.READ_LOCK);

                } else {
                    // resource points to a document
                    final DocumentImpl theDoc = getDocument(broker, parentCollectionURI, resourceURI);
                    trigger.afterUpdateDocumentMetadata(broker, txn, theDoc);
                    theDoc.getUpdateLock().release(Lock.READ_LOCK);
                }
            }

            // Return identification
            return new StringValue("foobar");

        } catch (final XPathException ex) {
            LOG.error(ex.getMessage());
            ex.setLocation(this.line, this.column, this.getSource());
            throw ex;

        } catch (final Throwable t) {
            LOG.error(t);

            throw new XPathException(this, t);
        }
    }


    private Map<String, List<?>> getParameters(DBBroker broker, Collection parentCollection) throws LockException, CollectionConfigurationException, EXistException, PermissionDeniedException {

        CollectionConfigurationManager cmm = new CollectionConfigurationManager(broker);
        CollectionConfiguration config = cmm.getOrCreateCollectionConfiguration(broker, parentCollection);

        List<TriggerProxy<? extends DocumentTrigger>> triggerProxies = config.documentTriggers();
//        for(TriggerProxy proxy : triggerProxies){
//            proxy.
//        }


        Element triggerElement = null;
        final NodeList nlParameter = triggerElement.getElementsByTagNameNS(NAMESPACE, "parameter");
        final Map<String, List<? extends Object>> parameters = ParametersExtractor.extract(nlParameter);

        return null;
    }

    private Collection getCollection(DBBroker broker, XmldbURI collectionURI) throws XPathException, PermissionDeniedException {

        Collection collection = broker.openCollection(collectionURI, Lock.READ_LOCK);
        if (collection == null) {
            throw new XPathException(String.format("Collection not found: %s", collectionURI));
        }
        return collection;

    }

    private boolean isCollection(DBBroker broker, XmldbURI collectionURI) throws XPathException, PermissionDeniedException {

        Collection collection = broker.openCollection(collectionURI, Lock.READ_LOCK);
        if (collection != null) {
            collection.getLock().release(Lock.READ_LOCK);
            return true;
        }
        return false;

    }

    private DocumentImpl getDocument(DBBroker broker, XmldbURI collectionURI, XmldbURI documentUri) throws XPathException, PermissionDeniedException {

        // Open collection if possible, else abort
        Collection collection = broker.openCollection(collectionURI, Lock.READ_LOCK);
        if (collection == null) {
            throw new XPathException(String.format("Collection does not exist %s", collectionURI));

        }

        // Open document if possible, else abort
        final DocumentImpl resource = collection.getDocument(broker, documentUri);
        if (resource == null) {
            collection.getLock().release(Lock.READ_LOCK);
            throw new XPathException(String.format("No resource found for path: %s", documentUri));
        }

        return resource;

    }
}
