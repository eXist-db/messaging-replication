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

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.exist.collections.Collection;
import org.exist.collections.triggers.CollectionTrigger;
import org.exist.collections.triggers.FilteringTrigger;
import org.exist.collections.triggers.TriggerException;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.jms.shared.eXistMessage;
import org.exist.jms.replication.shared.MessageHelper;
import org.exist.jms.replication.shared.TransportException;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.Txn;
import org.exist.xmldb.XmldbURI;

/**
 * Trigger for detecting document and collection changes to have the changes
 * propagated to remote eXist-db instances.
 *
 * @author Dannes Wessels (dannes@exist-db.org)
 */
public class ReplicationTrigger extends FilteringTrigger implements CollectionTrigger {

    private final static Logger LOGGER = LogManager.getLogger(ReplicationTrigger.class);
    
    private static final String BLOCKED_MESSAGE = "Blocked replication trigger for %s: was received by replication extension.";
    public static final String JMS_EXTENSION_PKG = "org.exist.jms";
    
    private Map<String, List<?>> parameters;

    private boolean isOriginIdAvailable = false;

    /**
     * Constructor.
     *
     * Verifies if new-enough version of eXist-db is used.
     *
     */
    public ReplicationTrigger() {

        super();

        try {
            // Verify if method does exist
            Class.forName("org.exist.Transaction");

            // Yes :-)
            isOriginIdAvailable = true;

        } catch (final java.lang.ClassNotFoundException error) {

            // Running an old version of eXist-db
            LOGGER.info("Method Txn.getOriginId() is not available. Please upgrade to eXist-db 2.2 or newer. " + error.getMessage());
        }

    }

    /**
     * Verify if the transaction is started by the JMX extension
     *
     * @param transaction The original transaction
     *
     * @return TRUE when started from JMS else FALSE.
     */
    private boolean isJMSOrigin(final Txn transaction) {

        // only try to get OriginId when metjod is available.
        final String originId = isOriginIdAvailable ? transaction.getOriginId() : null;

        return StringUtils.startsWith(originId, JMS_EXTENSION_PKG);
    }

    //
    // Document Triggers
    //
    private void afterUpdateCreateDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document,
                                           final eXistMessage.ResourceOperation operation) /* throws TriggerException */ {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(document.getURI().toString());
        }
        
        /** TODO: make optional? (for lJO) */
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, document.getURI().toString()));
            return;
        }

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
            //throw new TriggerException("Unable to retrieve message payload: " + ex.getMessage());
        }

        // Send Message   
        sendMessage(msg);
    }
    
    @Override
    public void afterCreateDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document) throws TriggerException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(document.getURI().toString());
        }
        
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, document.getURI().toString()));
            return;
        }

        this.afterUpdateCreateDocument(broker, transaction, document, eXistMessage.ResourceOperation.CREATE);
    }

    @Override
    public void afterUpdateDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document) throws TriggerException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(document.getURI().toString());
        }
        
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, document.getURI().toString()));
            return;
        }

        this.afterUpdateCreateDocument(broker, transaction, document, eXistMessage.ResourceOperation.UPDATE);
    }

    @Override
    public void afterCopyDocument(final DBBroker broker, final Txn transaction, final DocumentImpl document, final XmldbURI oldUri) throws TriggerException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%s %s", document.getURI().toString(), oldUri.toString()));
        }
        
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, document.getURI().toString()));
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

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%s %s", document.getURI().toString(), oldUri.toString()));
        }
        
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, document.getURI().toString()));
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

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(uri.toString());
        }
        
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, uri.toString()));
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
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(collection.getURI().toString());
        }
        
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, collection.getURI().toString()));
        }

        // Create Message
        final eXistMessage msg = new eXistMessage();
        msg.setResourceType(eXistMessage.ResourceType.COLLECTION);
        msg.setResourceOperation(eXistMessage.ResourceOperation.CREATE);
        msg.setResourcePath(collection.getURI().toString());

        final Map<String, Object> md = msg.getMetadata();
        MessageHelper.retrievePermission(md, collection.getPermissions());

        // Send Message   
        sendMessage(msg);
    }

    @Override
    public void afterCopyCollection(final DBBroker broker, final Txn transaction, final Collection collection, final XmldbURI oldUri) throws TriggerException {
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%s %s", collection.getURI().toString(), oldUri.toString()));
        }
        
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, collection.getURI().toString()));
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
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%s %s", collection.getURI().toString(), oldUri.toString()));
        }
        
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, collection.getURI().toString()));
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(uri.toString());
        }
        
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, uri.toString()));
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
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(document.getURI().toString());
        }

        /*
         * If the action is originated from a trigger, do not process it again
         */
        if (isJMSOrigin(transaction)) {
            LOGGER.info(String.format(BLOCKED_MESSAGE, document.getURI().toString()));
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
