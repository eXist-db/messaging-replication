/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2010 The eXist Project
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
package org.exist.jms.replication.subscribe;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.collections.IndexInfo;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.jms.replication.shared.MessageHelper;
import org.exist.jms.replication.shared.ReplicationTxnManager;
import org.exist.jms.shared.Constants;
import org.exist.jms.shared.Identity;
import org.exist.jms.shared.Report;
import org.exist.jms.shared.eXistMessage;
import org.exist.jms.shared.eXistMessagingListener;
import org.exist.security.Account;
import org.exist.security.Group;
import org.exist.security.Permission;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.storage.txn.TransactionManager;
import org.exist.storage.txn.Txn;
import org.exist.util.MimeTable;
import org.exist.util.MimeType;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.InputSource;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

/**
 * JMS listener for receiving JMS replication messages
 *
 * @author Dannes Wessels
 */
public class ReplicationJmsListener extends eXistMessagingListener {

    private final static Logger LOG = LogManager.getLogger(ReplicationJmsListener.class);
    private final BrokerPool brokerPool;
    private final org.exist.security.SecurityManager securityManager;
    private final TransactionManager txnManager;

    private final String localID;
    private final Report report;

    /**
     * Constructor
     *
     * @param brokerpool Reference to database broker pool
     */
    public ReplicationJmsListener(final BrokerPool brokerpool) {
        this.brokerPool = brokerpool;
        this.securityManager = brokerpool.getSecurityManager();
        this.txnManager = brokerpool.getTransactionManager();
        this.localID = Identity.getInstance().getIdentity();
        this.report = getReport();
    }

    /**
     * Set origin of transaction
     *
     * @param transaction The eXist-db transaction
     */
    private void setOrigin(final Txn transaction) {
        ReplicationTxnManager.addReplicationTransaction(transaction);
    }


    @Override
    public void onMessage(final Message msg) {

        final int receiverID = getReceiverID();

        // New Message is coming in
        report.incMessageCounterTotal();

        LOG.debug("Receiver={} nr={}", receiverID, report.getMessageCounterTotal());

        // Start reporting
        report.start();

        try {
            // Detect if the sender of the incoming message is the receiver
            if (StringUtils.isNotEmpty(localID)) {
                final String remoteID = msg.getStringProperty(Constants.EXIST_INSTANCE_ID);
                if (localID.equals(remoteID)) {
                    LOG.info("Incoming JMS messsage was originally sent by this same instance (jms.id={}). Stopped processing.", localID);
                    msg.acknowledge();
                    return; // Do not throw exeception; Receive was OK
                }
            }

            if (msg instanceof BytesMessage) {

                // Prepare received message
                final eXistMessage em = convertMessage((BytesMessage) msg);

                final Enumeration e = msg.getPropertyNames();
                while (e.hasMoreElements()) {
                    final Object next = e.nextElement();
                    if (next instanceof String) {
                        em.getMetadata().put((String) next, msg.getObjectProperty((String) next));
                    }
                }

                // Report some details into logging
                if (LOG.isDebugEnabled()) {
                    LOG.debug(em.getFullReport());
                } else {
                    LOG.info(em.getReport());
                }

                // First step: distinct between update for documents and messsages
                switch (em.getResourceType()) {
                    case DOCUMENT:
                        handleDocument(em);
                        break;
                    case COLLECTION:
                        handleCollection(em);
                        break;
                    default:
                        // We need to ack the message
                        msg.acknowledge();

                        final String errorMessage = String.format("Unknown resource type %s", em.getResourceType());
                        LOG.error(errorMessage);
                        throw new MessageReceiveException(errorMessage);
                }
                report.incMessageCounterOK();

            } else {
                // We need to ack the message
                msg.acknowledge();

                // Only ByteMessage objects supported. 
                throw new MessageReceiveException(String.format("Could not handle message type %s", msg.getClass().getSimpleName()));
            }

            // We need to ack the message
            msg.acknowledge();

        } catch (final MessageReceiveException ex) {
            // Thrown by local code. Just make it pass\
            report.addListenerError(ex);
            LOG.error("Could not handle received message: {}", ex.getMessage(), ex);
            throw ex;

        } catch (final Throwable t) {
            // Something really unexpected happened. Report it
            report.addListenerError(t);
            LOG.error(t.getMessage(), t);
            throw new MessageReceiveException(String.format("Could not handle received message: %s", t.getMessage()), t);

        } finally {
            // update statistics
            report.stop();
            report.addCumulatedProcessingTime();
        }
    }

    //
    // The code below handles the incoming message ; DW: should be moved to separate class
    //

    /**
     * Convert JMS ByteMessage into an eXist-db specific message.
     *
     * @param bm The original message
     * @return The converted message
     */
    private eXistMessage convertMessage(final BytesMessage bm) {
        final eXistMessage em = new eXistMessage();

        try {
            String value = bm.getStringProperty(eXistMessage.EXIST_RESOURCE_TYPE);
            em.setResourceType(value);

            value = bm.getStringProperty(eXistMessage.EXIST_RESOURCE_OPERATION);
            em.setResourceOperation(value);

            value = bm.getStringProperty(eXistMessage.EXIST_SOURCE_PATH);
            em.setResourcePath(value);

            value = bm.getStringProperty(eXistMessage.EXIST_DESTINATION_PATH);
            em.setDestinationPath(value);

            // This is potentially memory intensive
            final long size = bm.getBodyLength();
            final byte[] payload = new byte[(int) size];
            bm.readBytes(payload);
            em.setPayload(payload);

        } catch (final JMSException ex) {
            final String errorMessage = String.format("Unable to convert incoming message. (%s):  %s", ex.getErrorCode(), ex.getMessage());
            LOG.error(errorMessage, ex);
            throw new MessageReceiveException(errorMessage);

        } catch (final IllegalArgumentException ex) {
            final String errorMessage = String.format("Unable to convert incoming message. %s", ex.getMessage());
            LOG.error(errorMessage, ex);
            throw new MessageReceiveException(errorMessage);
        }

        return em;
    }

    /**
     * Handle operation on documents
     *
     * @param em Message containing information about documents
     */
    private void handleDocument(final eXistMessage em) {

        switch (em.getResourceOperation()) {
            case CREATE:
            case UPDATE:
                createUpdateDocument(em);
                break;

            case METADATA:
                updateMetadataDocument(em);
                break;

            case DELETE:
                deleteDocument(em);
                break;

            case MOVE:
                relocateDocument(em, false);
                break;

            case COPY:
                relocateDocument(em, true);
                break;

            default:
                final String errorMessage = String.format("Unknown resource type %s", em.getResourceOperation());
                LOG.error(errorMessage);
                throw new MessageReceiveException(errorMessage);
        }
    }

    /**
     * Handle operation on collections
     *
     * @param em Message containing information about collections
     */
    private void handleCollection(final eXistMessage em) {

        switch (em.getResourceOperation()) {
            case CREATE:
            case UPDATE:
                createCollection(em);
                break;

            case METADATA:
                updateMetadataCollection(em);
                break;

            case DELETE:
                deleteCollection(em);
                break;

            case MOVE:
                relocateCollection(em, false);
                break;

            case COPY:
                relocateCollection(em, true);
                break;

            default:
                final String errorMessage = "Unknown change type";
                LOG.error(errorMessage);
                throw new MessageReceiveException(errorMessage);
        }
    }

    /**
     * Created document in database
     */
    private void createUpdateDocument(final eXistMessage em) {

        final Map<String, Object> metaData = em.getMetadata();

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());
        final XmldbURI colURI = sourcePath.removeLastSegment();
        final XmldbURI docURI = sourcePath.lastSegment();


        // Get mime, or NULL when not available
        MimeType mime = MimeTable.getInstance().getContentTypeFor(docURI.toString());
        if (mime == null) {
            mime = MimeType.BINARY_TYPE;
        }

        // Get OWNER and Group
        final Optional<String> userName = getOrCreateUserName(metaData);
        final Optional<String> groupName = getOrCreateGroupName(metaData);

        // Get MIME_TYPE
        final String mimeType = getMimeType(metaData, mime.getName());

        // Get MODE
        final Optional<Integer> mode = getMode(metaData);

        // Last modified
        final Optional<Long> lastModified = getLastModified(metaData);
        final Optional<Long> createTime = getCreationTime(metaData);

        // Check for collection, create if not existent
        try {
            createOrCheckCollection(colURI);

        } catch (final MessageReceiveException e) {
            LOG.error(e.getMessage(), e);
            e.setExistMessage(em);
            throw e;

        } catch (final Throwable t) {
            if (LOG.isDebugEnabled()) {
                LOG.error(t.getMessage(), t);
            } else {
                LOG.error(t.getMessage());
            }
            throw new MessageReceiveException(String.format("Unable to create collection in database: %s", t.getMessage()), em);
        }


        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             final Txn txn = txnManager.beginTransaction();
             final Collection collection = broker.openCollection(colURI, Lock.LockMode.WRITE_LOCK)) {

            if (collection == null) {
                throw new MessageReceiveException("Collection " + sourcePath + " does not exist", em);
            }

            setOrigin(txn);

            final DocumentImpl doc;
            if (mime.isXMLType()) {

                InputSource inputsource = null;

                // Stream into database
                try (final InputStream byteInputStream = new ByteArrayInputStream(em.getPayload())) {

                    // DW: future improvement: determine compression based on property.
                    GZIPInputStream gis = new GZIPInputStream(byteInputStream);
                    inputsource = new InputSource(gis);

                    // DW: collection can be null?
                    final IndexInfo info = collection.validateXMLResource(txn, broker, docURI, inputsource);
                    doc = info.getDocument();
                    doc.getMetadata().setMimeType(mimeType);

                    // reconstruct gzip input stream
                    byteInputStream.reset();
                    gis = new GZIPInputStream(byteInputStream);
                    inputsource = new InputSource(gis);

                    collection.store(txn, broker, info, inputsource);


                } finally {
                    if (inputsource != null) {
                        inputsource.getByteStream().close();
                    }
                }

            } else {

                // Stream into database
                try (final ByteArrayInputStream bais = new ByteArrayInputStream(em.getPayload());
                     final GZIPInputStream gis = new GZIPInputStream(bais);
                     final BufferedInputStream bis = new BufferedInputStream(gis)) {
                    // DW: collection can be null
                    doc = collection.addBinaryResource(txn, broker, docURI, bis, mimeType, em.getPayload().length);
                }
            }

            // Set owner,group and permissions
            final Permission permission = doc.getPermissions();
            if (userName.isPresent()) {
                permission.setOwner(userName.get());
            }
            if (groupName.isPresent()) {
                permission.setGroup(groupName.get());
            }
            if (mode.isPresent()) {
                permission.setMode(mode.get());
            }

            // Set dates
            lastModified.ifPresent(aLong -> doc.getMetadata().setLastModified(aLong));
            createTime.ifPresent(aLong -> doc.getMetadata().setCreated(aLong));

            // Commit change
            txn.commit();


        } catch (final Throwable ex) {

            if (LOG.isDebugEnabled()) {
                LOG.error(ex.getMessage(), ex);
            } else {
                LOG.error(ex.getMessage());
            }

            throw new MessageReceiveException(String.format("Unable to write document into database. Reason: %s", ex.getMessage()), em);

        }
    }

    /**
     * Metadata is updated in database
     * <p>
     */
    private void updateMetadataDocument(final eXistMessage em) {
        // Permissions
        // Mimetype
        // owner/groupname

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());
        final XmldbURI colURI = sourcePath.removeLastSegment();
        final XmldbURI docURI = sourcePath.lastSegment();

        // Get mime, or binary type when not available
        MimeType mime = MimeTable.getInstance().getContentTypeFor(docURI.toString());
        if (mime == null) {
            mime = MimeType.BINARY_TYPE;
        }


        // References to the database
        final DocumentImpl resource;

        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             final Txn txn = txnManager.beginTransaction();
             final Collection collection = broker.openCollection(colURI, Lock.LockMode.WRITE_LOCK)) {

            setOrigin(txn);

            // Open collection if possible, else abort
            if (collection == null) {
                LOG.error("Collection does not exist {}", colURI);
                txn.abort();
                return; // be silent
            }

            // Open document if possible, else abort
            resource = collection.getDocument(broker, docURI);
            if (resource == null) {
                LOG.error("No resource found for path: {}", sourcePath);
                txn.abort();
                return; // be silent
            }

            // Get supplied metadata
            final Map<String, Object> metaData = em.getMetadata();

            final Permission perms = resource.getPermissions();

            final Optional<String> userName = getOrCreateUserName(metaData);
            if (userName.isPresent()) {
                perms.setOwner(userName.get());
            }

            final Optional<String> groupName = getOrCreateGroupName(metaData);
            if (groupName.isPresent()) {
                perms.setGroup(groupName.get());
            }

            final Optional<Integer> mode = getMode(metaData);
            if (mode.isPresent()) {
                perms.setMode(mode.get());
            }

            final String mimeType = getMimeType(metaData, mime.getName());
            if (mimeType != null) {
                resource.getMetadata().setMimeType(mimeType);
            }

            final Optional<Long> createTime = getCreationTime(metaData);
            createTime.ifPresent(aLong -> resource.getMetadata().setCreated(aLong));

            final Optional<Long> lastModified = getLastModified(metaData);
            lastModified.ifPresent(aLong -> resource.getMetadata().setLastModified(aLong));

            // Make persistent
            broker.storeMetadata(txn, resource);

            // Commit change
            txn.commit();

        } catch (final Throwable e) {
            LOG.error(e.getMessage(), e);
            throw new MessageReceiveException(e.getMessage(), e, em);
        }

    }

    /**
     * Remove document from database. If a document or collection does not exist, this is logged.
     */
    private void deleteDocument(final eXistMessage em) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());
        final XmldbURI colURI = sourcePath.removeLastSegment();
        final XmldbURI docURI = sourcePath.lastSegment();


        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             final Txn txn = txnManager.beginTransaction();
             final Collection collection = broker.openCollection(colURI, Lock.LockMode.WRITE_LOCK)) {

            setOrigin(txn);

            // Open collection if possible, else abort
            if (collection == null) {
                LOG.error("Collection does not exist {}", colURI);
                txn.abort();
                return; // silently ignore
            }

            // Open document if possible, else abort
            final DocumentImpl resource = collection.getDocument(broker, docURI);
            if (resource == null) {
                LOG.error("No resource found for path: {}", sourcePath);
                txn.abort();
                return; // silently ignore
            }

            // This delete is based on mime-type /ljo 
            if (resource.getResourceType() == DocumentImpl.BINARY_FILE) {
                collection.removeBinaryResource(txn, broker, resource.getFileURI());

            } else {
                collection.removeXMLResource(txn, broker, resource.getFileURI());
            }

            // Commit change
            txn.commit();

        } catch (final Throwable t) {

            if (LOG.isDebugEnabled()) {
                LOG.error(t.getMessage(), t);
            } else {
                LOG.error(t.getMessage());
            }

            throw new MessageReceiveException(t.getMessage(), t, em);

        }
    }

    /**
     * Remove collection from database
     */
    private void deleteCollection(final eXistMessage em) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());


        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             final Txn txn = txnManager.beginTransaction();
             final Collection collection = broker.openCollection(sourcePath, Lock.LockMode.WRITE_LOCK)) {

            setOrigin(txn);

            // Open collection if possible, else abort

            if (collection == null) {
                LOG.error("Collection does not exist: {}", sourcePath);
                txn.abort();
                return;  // be silent
            }

            // Remove collection
            broker.removeCollection(txn, collection);

            // Commit change
            txn.commit();

        } catch (final Throwable t) {

            if (LOG.isDebugEnabled()) {
                LOG.error(t.getMessage(), t);
            } else {
                LOG.error(t.getMessage());
            }

            throw new MessageReceiveException(t.getMessage(), em);
        }
    }

    /**
     * Created collection in database
     */
    private void createCollection(final eXistMessage em) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());

        final Map<String, Object> metaData = em.getMetadata();

        // Get OWNER/GROUP/MODE
        final Optional<String> userName = getOrCreateUserName(metaData);
        final Optional<String> groupName = getOrCreateGroupName(metaData);

        final Optional<Integer> mode = getMode(metaData);
        final Optional<Long> createTime = getCreationTime(metaData);

        // Be sure collection exists
        createOrCheckCollection(sourcePath);

        // Update meta data
        try (final Collection collection = updateCollectionMetadata(sourcePath, userName, groupName, mode, createTime)) {
            LOG.info("Updated meta data for {}", collection.getURI().toString());
        }
    }

    /**
     * Create new collection when required.
     */
    private void createOrCheckCollection(final XmldbURI sourcePath) throws MessageReceiveException {

        // New collection to be created
        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             final Txn txn = txnManager.beginTransaction();
             final Collection collection = broker.getOrCreateCollection(txn, sourcePath)) {

            setOrigin(txn);

            // Create collection when required
            if (collection == null) {
                throw new MessageReceiveException("Collection " + sourcePath + " does not exist or could not be created");
            }

            broker.saveCollection(txn, collection);

            // Commit change
            txn.commit();


        } catch (final Throwable t) {

            if (LOG.isDebugEnabled()) {
                LOG.error(t.getMessage(), t);
            } else {
                LOG.error(t.getMessage());
            }

            throw new MessageReceiveException(t.getMessage(), t);

        }

    }

    /**
     * Create new collection when required, or force update meta-data when already present.
     */
    private Collection updateCollectionMetadata(final XmldbURI sourcePath, final Optional<String> userName,
                                                final Optional<String> groupName, final Optional<Integer> mode,
                                                final Optional<Long> createTime) throws MessageReceiveException {


        // New collection to be created
        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             final Txn txn = txnManager.beginTransaction();
             final Collection collection = broker.openCollection(sourcePath, Lock.LockMode.WRITE_LOCK)) {

            setOrigin(txn);

            // Create collection when required
            if (collection == null) {
                throw new MessageReceiveException("Collection " + sourcePath + " does not exist");
            }


            // Set owner,group and permissions
            final Permission permission = collection.getPermissions();
            if (userName.isPresent()) {
                permission.setOwner(userName.get());
            }
            if (groupName.isPresent()) {
                permission.setGroup(groupName.get());
            }
            if (mode.isPresent()) {
                permission.setMode(mode.get());
            }
            // Set Create time only
            createTime.ifPresent(collection::setCreated);

            broker.saveCollection(txn, collection);

            // Commit change
            txn.commit();

            // ToDO... possible?
            return collection;

        } catch (final Throwable t) {

            if (LOG.isDebugEnabled()) {
                LOG.error(t.getMessage(), t);
            } else {
                LOG.error(t.getMessage());
            }

            throw new MessageReceiveException(t.getMessage(), t);

        }
    }


    private void relocateDocument(final eXistMessage em, final boolean keepDocument) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());
        final XmldbURI sourceColURI = sourcePath.removeLastSegment();
        final XmldbURI sourceDocURI = sourcePath.lastSegment();

        final XmldbURI destPath = XmldbURI.create(em.getDestinationPath());
        final XmldbURI destColURI = destPath.removeLastSegment();
        final XmldbURI destDocURI = destPath.lastSegment();


        final DocumentImpl srcDocument;


        // Use the correct lock
        final Lock.LockMode lockTypeOriginal = keepDocument ? Lock.LockMode.READ_LOCK : Lock.LockMode.WRITE_LOCK;

        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             final Txn txn = txnManager.beginTransaction();
             final Collection srcCollection = broker.openCollection(sourceColURI, lockTypeOriginal);
             final Collection destCollection = broker.openCollection(destColURI, Lock.LockMode.WRITE_LOCK)) {

            setOrigin(txn);

            // Open collection if possible, else abort
            if (srcCollection == null) {
                LOG.error("Collection not found: {}", sourceColURI);
                txn.abort();
                return; // be silent
            }

            // Open document if possible, else abort
            srcDocument = srcCollection.getDocument(broker, sourceDocURI);
            if (srcDocument == null) {
                LOG.error("No resource found for path: {}", sourcePath);
                txn.abort();
                return; // be silent
            }

            // Open collection if possible, else abort
            if (destCollection == null) {
                LOG.error("Destination collection {} does not exist.", destColURI);
                txn.abort();
                return; // be silent
            }

            // Perform actual move/copy
            if (keepDocument) {
                broker.copyResource(txn, srcDocument, destCollection, destDocURI);
            } else {
                broker.moveResource(txn, srcDocument, destCollection, destDocURI);
            }

            // Commit change
            txnManager.commit(txn);

        } catch (final Throwable e) {
            LOG.error(e.getMessage(), e);
            throw new MessageReceiveException(e.getMessage(), e, em);

        }
    }

    private void relocateCollection(final eXistMessage em, final boolean keepCollection) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());

        final XmldbURI destPath = XmldbURI.create(em.getDestinationPath());
        final XmldbURI destColURI = destPath.removeLastSegment();
        final XmldbURI destDocURI = destPath.lastSegment();

//        Collection srcCollection = null;
//        Collection destCollection = null;

        // Use the correct lock
        final Lock.LockMode lockTypeOriginal = keepCollection ? Lock.LockMode.READ_LOCK : Lock.LockMode.WRITE_LOCK;

        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             final Txn txn = txnManager.beginTransaction();
             final Collection srcCollection = broker.openCollection(sourcePath, lockTypeOriginal);
             final Collection destCollection = broker.openCollection(destColURI, Lock.LockMode.WRITE_LOCK)) {

            setOrigin(txn);

            // Open source collection if possible, else abort
            if (srcCollection == null) {
                LOG.error("Source collection {} does not exist.", sourcePath);
                txn.abort();
                return; // be silent
            }

            // Open destination collection if possible, else abort
            // TODO: potential improvement: create collection, risk: permissions of parent collections.
            if (destCollection == null) {
                LOG.error("Destination collection {} does not exist.", destColURI);
                txn.abort();
                return; // be silent
            }

            // Perform actual move/copy
            if (keepCollection) {
                broker.copyCollection(txn, srcCollection, destCollection, destDocURI);
            } else {
                broker.moveCollection(txn, srcCollection, destCollection, destDocURI);
            }

            // Commit change
            txn.commit();

        } catch (final Throwable e) {
            LOG.error(e.getMessage(), e);
            throw new MessageReceiveException(e.getMessage(), em);

        }
    }

    @Override
    public String getUsageType() {
        return "replication";
    }

    private void updateMetadataCollection(final eXistMessage em) {
        final XmldbURI sourceColURI = XmldbURI.create(em.getResourcePath());

        final Map<String, Object> metaData = em.getMetadata();

        // Get OWNER/GROUP/MODE
        final Optional<String> userName = getOrCreateUserName(metaData);
        final Optional<String> groupName = getOrCreateGroupName(metaData);

        final Optional<Integer> mode = getMode(metaData);

        final Optional<Long> created = getCreationTime(metaData);


        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             final Txn txn = txnManager.beginTransaction();
             final Collection collection = broker.openCollection(sourceColURI, Lock.LockMode.WRITE_LOCK)) {

            setOrigin(txn);

            // Open collection if possible, else abort
            if (collection == null) {
                LOG.error("Collection not found: {}", sourceColURI);
                txnManager.abort(txn);
                return; // be silent
            }

            final Permission permission = collection.getPermissions();
            if (userName.isPresent()) {
                permission.setOwner(userName.get());
            }
            if (groupName.isPresent()) {
                permission.setGroup(groupName.get());
            }
            if (mode.isPresent()) {
                permission.setMode(mode.get());
            }

            created.ifPresent(collection::setCreated);

            // Make persistent
            broker.saveCollection(txn, collection);

            // Commit change
            txn.commit();

        } catch (final Throwable e) {
            LOG.error(e.getMessage(), e);
            throw new MessageReceiveException(e.getMessage(), em);

        }

    }


    /**
     * Get valid username for database, else system subject if not valid. If no username is supplied,
     * the ownership of a resource shall not be touched.
     */
    private Optional<String> getOrCreateUserName(final Map<String, Object> metaData) {

        final String userName;
        final Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_OWNER);
        if (prop instanceof String) {
            userName = (String) prop;
        } else {
            LOG.debug("No username provided");
            return Optional.empty();
        }

        Account account = securityManager.getAccount(userName);
        if (account == null) {
            LOG.error("Username {} does not exist.", userName);

//            final Account user = new UserAider(userName);
//            try {
//                securityManager.addAccount(user);
//                account = user;
//            } catch (PermissionDeniedException | EXistException e) {
//                LOG.error(String.format("Unable to create user %s. Fall back to default. %s", userName,e.getMessage()));
//            }
//
//
//        }
//
//        // Fallback
//        if (account == null) {
            account = securityManager.getSystemSubject();
        }

        return Optional.of(account.getName());
    }

    /**
     * Get valid groupname for database for database, else system subject if not existent. If no groupname is supplied,
     * the ownership of a resource shall not be touched.
     */
    private Optional<String> getOrCreateGroupName(final Map<String, Object> metaData) {

        final String groupName;
        final Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_GROUP);
        if (prop instanceof String) {
            groupName = (String) prop;
        } else {
            LOG.debug("No groupname provided");
            return Optional.empty();
        }

        Group group = securityManager.getGroup(groupName);
        if (group == null) {
            LOG.info("Group {} does not exist.", groupName);
//
//            try {
//                Group newGroup = new GroupAider(groupName);
//                securityManager.addGroup(newGroup);
//                group = newGroup;
//            } catch (PermissionDeniedException | EXistException e) {
//                LOG.error(String.format("Unable to create group %s. Fall back to default. %s", groupName, e.getMessage()));
//            }
//        }
//
//        // Fallback
//        if (group == null) {
            group = securityManager.getSystemSubject().getDefaultGroup();
        }

        return Optional.of(group.getName());
    }

    private Optional<Long> getLastModified(final Map<String, Object> metaData) {

        final Long lastModified;
        final Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_LASTMODIFIED);
        if (prop instanceof Long) {
            lastModified = (Long) prop;
        } else {
            LOG.debug("No lastmodified provided");
            return Optional.empty();
        }

        return Optional.of(lastModified);

    }

    private Optional<Long> getCreationTime(final Map<String, Object> metaData) {

        final Long creationTime;
        final Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_CREATIONTIME);
        if (prop instanceof Long) {
            creationTime = (Long) prop;
        } else {
            LOG.debug("No creationtime provided");
            return Optional.empty();
        }

        return Optional.of(creationTime);

    }

    private String getMimeType(final Map<String, Object> metaData, final String defaultName) {

        final MimeTable mimeTable = MimeTable.getInstance();
        String mimeType = null;
        final Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_MIMETYPE);
        if (prop instanceof String) {
            final MimeType mT = mimeTable.getContentType((String) prop);
            if (mT != null) {
                mimeType = mT.getName();
            }
        }

        // Fallback based on default
        if (mimeType == null) {
            mimeType = defaultName;
        }

        return mimeType;

    }

    private Optional<Integer> getMode(final Map<String, Object> metaData) {
        // Get/Set permissions
        final Integer mode;
        final Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_MODE);
        if (prop instanceof Integer) {
            mode = (Integer) prop;
        } else {
            LOG.debug("No mode provided");
            return java.util.Optional.empty();
        }
        return Optional.of(mode);
    }


}
