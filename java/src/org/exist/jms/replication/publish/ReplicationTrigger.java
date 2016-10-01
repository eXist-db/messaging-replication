/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2012 The eXist Project
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
 *
 *  $Id$
 */
package org.exist.jms.replication.publish;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.collections.triggers.CollectionTrigger;
import org.exist.collections.triggers.DocumentTrigger;
import org.exist.collections.triggers.SAXTrigger;
import org.exist.collections.triggers.TriggerException;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.jms.replication.shared.MessageHelper;
import org.exist.jms.replication.shared.ReplicationGuard;
import org.exist.jms.replication.shared.TransportException;
import org.exist.jms.shared.eXistMessage;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.Txn;
import org.exist.xmldb.XmldbURI;

import java.util.List;
import java.util.Map;

/**
 * Trigger for detecting document and collection changes to have the changes
 * propagated to remote eXist-db instances.
 *
 * @author Dannes Wessels (dannes@exist-db.org)
 */
public class ReplicationTrigger extends SAXTrigger implements DocumentTrigger, CollectionTrigger {

    public static final String JMS_EXTENSION_PKG = "org.exist.jms";
    public static final String REPLICATION_OFF = "Resource operation not replicated: replication is switched off";
    public static final String BLOCKED_MESSAGE = "Prevented re-replication of '{}'";
    private final static Logger LOGGER = LogManager.getLogger(ReplicationTrigger.class);
    private final ReplicationGuard guard = ReplicationGuard.getInstance();
    private Map<String, List<?>> parameters;

    /**
     * Verify if the transaction is started by the JMX extension
     *
     * @param transaction The original transaction
     * @return TRUE when started from the eXist-db JMS else FALSE.
     */
    private boolean isJMSOrigin(final Txn transaction) {

        // Get originId.
        final String originId = transaction.getOriginId();

        return StringUtils.startsWith(originId, JMS_EXTENSION_PKG);
    }

    //
    // Document Triggers
    //
    private void afterUpdateCreateDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document,
                                           final eXistMessage.ResourceOperation operation) /* throws TriggerException */ {

        // note: checks and logs are done in calling methods.

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.DOCUMENT);
        msg.setResourceOperation(operation);
        msg.setResourcePath(document.getURI().toString());

        // Retrieve Metadata
        final Map<String, Object> md = msg.getMetadata();
        MessageHelper.retrieveDocMetadata(md, document.getMetadata());
        MessageHelper.retrieveFromDocument(md, document);
        MessageHelper.retrievePermission(md, document.getPermissions());


        // The content is always gzip-ped
        md.put(MessageHelper.EXIST_MESSAGE_CONTENTENCODING, "gzip");

        // Serialize document
        try {
            msg.setPayload(MessageHelper.gzipSerialize(broker, document));

        } catch (final Throwable ex) {
            LOGGER.error(String.format("Problem while serializing document (contentLength=%s) to compressed message:%s",
                    document.getContentLength(), ex.getMessage()), ex);
        }

        // Send Message   
        sendMessage(msg);
    }

    @Override
    public void afterCreateDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document) throws TriggerException {

        LOGGER.info("Create document '{}'", document.getURI().toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, document.getURI().toString());
            return;
        }

        this.afterUpdateCreateDocument(broker, transaction, document, eXistMessage.ResourceOperation.CREATE);
    }

    @Override
    public void afterUpdateDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document) throws TriggerException {

        LOGGER.info("Update document '{}'", document.getURI().toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, document.getURI().toString());
            return;
        }

        this.afterUpdateCreateDocument(broker, transaction, document, eXistMessage.ResourceOperation.UPDATE);
    }

    @Override
    public void afterCopyDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document, final XmldbURI oldUri) throws TriggerException {

        LOGGER.info("Copy document from '{}' to '{}'", oldUri.toString(), document.getURI().toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, document.getURI().toString());
            return;
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.DOCUMENT);
        msg.setResourceOperation(eXistMessage.ResourceOperation.COPY);
        msg.setResourcePath(oldUri.toString());
        msg.setDestinationPath(document.getURI().toString());

        // Send Message   
        sendMessage(msg);
    }

    @Override
    public void afterMoveDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document, final XmldbURI oldUri) throws TriggerException {

        LOGGER.info("Move document from '{}' to '{}'", oldUri.toString(), document.getURI().toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, document.getURI().toString());
            return;
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.DOCUMENT);
        msg.setResourceOperation(eXistMessage.ResourceOperation.MOVE);
        msg.setResourcePath(oldUri.toString());
        msg.setDestinationPath(document.getURI().toString());

        // Send Message   
        sendMessage(msg);
    }

    @Override
    public void afterDeleteDocument(final DBBroker broker, final Txn transaction, final XmldbURI uri) throws TriggerException {

        LOGGER.info("Delete document '{}'", uri.toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, uri.toString());
            return;
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.DOCUMENT);
        msg.setResourceOperation(eXistMessage.ResourceOperation.DELETE);
        msg.setResourcePath(uri.toString());

        // Send Message   
        sendMessage(msg);
    }

    //
    // Collection Triggers
    //
    @Override
    public void afterCreateCollection(final DBBroker broker, final Txn transaction, final Collection collection) throws TriggerException {

        LOGGER.info("Create collection '{}'", collection.getURI().toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, collection.getURI().toString());
            return;
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.COLLECTION);
        msg.setResourceOperation(eXistMessage.ResourceOperation.CREATE);
        msg.setResourcePath(collection.getURI().toString());

        final Map<String, Object> md = msg.getMetadata();
        MessageHelper.retrievePermission(md, collection.getPermissions());
        MessageHelper.retrieveFromCollection(md, collection);

        // Send Message   
        sendMessage(msg);
    }

    //@Override
    public void beforeUpdateCollectionMetadata(final DBBroker broker, final Txn txn, final Collection collection) throws TriggerException {
        // ignored
    }

    //@Override
    public void afterUpdateCollectionMetadata(final DBBroker broker, final Txn txn, final Collection collection) throws TriggerException {

        LOGGER.info("Update collection metadata '{}'", collection.getURI().toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(txn)) {
            LOGGER.info(BLOCKED_MESSAGE, collection.getURI().toString());
            return;
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.COLLECTION);
        msg.setResourceOperation(eXistMessage.ResourceOperation.METADATA);
        msg.setResourcePath(collection.getURI().toString());

        final Map<String, Object> md = msg.getMetadata();
        MessageHelper.retrievePermission(md, collection.getPermissions());
        MessageHelper.retrieveFromCollection(md, collection);

        // Send Message
        sendMessage(msg);
    }

    @Override
    public void afterCopyCollection(final DBBroker broker, final Txn transaction, final Collection collection, final XmldbURI oldUri) throws TriggerException {

        LOGGER.info("Copy collection from '{}' to '{}'", oldUri.toString(), collection.getURI().toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, collection.getURI().toString());
            return;
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.COLLECTION);
        msg.setResourceOperation(eXistMessage.ResourceOperation.COPY);
        msg.setResourcePath(oldUri.toString());
        msg.setDestinationPath(collection.getURI().toString());

        // Send Message   
        sendMessage(msg);
    }

    @Override
    public void afterMoveCollection(final DBBroker broker, final Txn transaction, final Collection collection, final XmldbURI oldUri) throws TriggerException {

        LOGGER.info("Move collection from '{}' to '{}'", oldUri.toString(), collection.getURI().toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, collection.getURI().toString());
            return;
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.COLLECTION);
        msg.setResourceOperation(eXistMessage.ResourceOperation.MOVE);
        msg.setResourcePath(oldUri.toString());
        msg.setDestinationPath(collection.getURI().toString());

        // Send Message   
        sendMessage(msg);
    }

    @Override
    public void afterDeleteCollection(final DBBroker broker, final Txn transaction, final XmldbURI uri) throws TriggerException {

        LOGGER.info("Delete collection '{}'", uri.toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, uri.toString());
            return;
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.COLLECTION);
        msg.setResourceOperation(eXistMessage.ResourceOperation.DELETE);
        msg.setResourcePath(uri.toString());

        // Send Message   
        sendMessage(msg);
    }

    // 
    // Metadata triggers
    //    

    @Override
    public void afterUpdateDocumentMetadata(final DBBroker broker, final Txn transaction, final DocumentImpl document) throws TriggerException {

        LOGGER.info("Update document metadata '{}'", document.getURI().toString());

        if (!guard.getReplicationEnabled()) {
            LOGGER.info(REPLICATION_OFF);
            return;
        }

        /*
         * If the action is originated from a trigger, do not process it again
         */
        if (isJMSOrigin(transaction)) {
            LOGGER.info(BLOCKED_MESSAGE, document.getURI().toString());
            return;
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.DOCUMENT);
        msg.setResourceOperation(eXistMessage.ResourceOperation.METADATA);
        msg.setResourcePath(document.getURI().toString());

        // Retrieve Metadata
        final Map<String, Object> md = msg.getMetadata();
        MessageHelper.retrieveDocMetadata(md, document.getMetadata());
        MessageHelper.retrieveFromDocument(md, document);
        MessageHelper.retrievePermission(md, document.getPermissions());

        // Send Message   
        sendMessage(msg);
    }

    //
    // Misc         
    //
    @Override
    public void configure(final DBBroker broker, final Collection parentCollection, final Map<String, List<?>> parameters) throws TriggerException {
        super.configure(broker, parentCollection, parameters);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Configuring replication trigger for collection '{}'", parentCollection.getURI());
        }

        this.parameters = parameters;

    }

    /**
     * Send 'trigger' message with parameters set using
     * {@link #configure(org.exist.storage.DBBroker, org.exist.collections.Collection, java.util.Map)}
     */
    private void sendMessage(final eXistMessage msg) /* throws TriggerException  */ {
        // Send Message   
        final JMSMessageSender sender = new JMSMessageSender(parameters);
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Sending JMS message for '{}' on '{}'", msg.getResourceOperation().toString(), msg.getResourcePath());
            }

            sender.sendMessage(msg);

        } catch (final TransportException ex) {
            LOGGER.error(ex.getMessage(), ex);
            //throw new TriggerException(ex.getMessage(), ex);

        } catch (final Throwable ex) {
            LOGGER.error(ex.getMessage(), ex);
            //throw new TriggerException(ex.getMessage(), ex);
        }
    }

    /*
     * ****** unused methods follow ******
     */
    //@Override
    @Deprecated
    public void prepare(final int event, final DBBroker broker, final Txn transaction,
                        final XmldbURI documentPath, final DocumentImpl existingDocument) throws TriggerException {
        // Ignored
    }

    //@Override
    @Deprecated
    public void finish(final int event, final DBBroker broker, final Txn transaction,
                       final XmldbURI documentPath, final DocumentImpl document) {
        // Ignored
    }

    @Override
    public void beforeCreateDocument(final DBBroker broker, final Txn transaction,
                                     final XmldbURI uri) throws TriggerException {
        // Ignored
    }

    @Override
    public void beforeUpdateDocument(final DBBroker broker, final Txn transaction,
                                     final DocumentImpl document) throws TriggerException {
        // Ignored
    }

    @Override
    public void beforeCopyDocument(final DBBroker broker, final Txn transaction,
                                   final DocumentImpl document, final XmldbURI newUri) throws TriggerException {
        // Ignored
    }

    @Override
    public void beforeMoveDocument(final DBBroker broker, final Txn transaction,
                                   final DocumentImpl document, final XmldbURI newUri) throws TriggerException {
        // Ignored
    }

    @Override
    public void beforeDeleteDocument(final DBBroker broker, final Txn transaction,
                                     final DocumentImpl document) throws TriggerException {
        // Ignored
    }

    @Override
    public void beforeUpdateDocumentMetadata(final DBBroker broker, final Txn txn, final DocumentImpl document) throws TriggerException {
        // Ignored
    }

    //@Override
    @Deprecated
    public void prepare(final int event, final DBBroker broker, final Txn transaction, final Collection collection,
                        final Collection newCollection) throws TriggerException {
        // Ignored
    }

    //@Override
    @Deprecated
    public void finish(final int event, final DBBroker broker, final Txn transaction, final Collection collection,
                       final Collection newCollection) {
        // Ignored
    }

    @Override
    public void beforeCreateCollection(final DBBroker broker, final Txn transaction,
                                       final XmldbURI uri) throws TriggerException {
        // Ignored
    }

    @Override
    public void beforeCopyCollection(final DBBroker broker, final Txn transaction, final Collection collection,
                                     final XmldbURI newUri) throws TriggerException {
        // Ignored
    }

    @Override
    public void beforeMoveCollection(final DBBroker broker, final Txn transaction, final Collection collection,
                                     final XmldbURI newUri) throws TriggerException {
        // Ignored
    }

    @Override
    public void beforeDeleteCollection(final DBBroker broker, final Txn transaction,
                                       final Collection collection) throws TriggerException {
        // Ignored
    }


}
