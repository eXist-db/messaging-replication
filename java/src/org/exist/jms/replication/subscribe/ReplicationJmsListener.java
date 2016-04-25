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
import org.exist.jms.shared.*;
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
import org.exist.util.VirtualTempFile;
import org.exist.util.VirtualTempFileInputSource;
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

    private String localID = null;
    private Report report = null;

    /**
     * Constructor
     *
     * @param brokerpool Reference to database broker pool
     */
    public ReplicationJmsListener(final BrokerPool brokerpool) {
        brokerPool = brokerpool;
        securityManager = brokerpool.getSecurityManager();
        txnManager = brokerpool.getTransactionManager();
        localID = Identity.getInstance().getIdentity();
        report = getReport();
    }

    /**
     * Set origin of transaction
     *
     * @param transaction The eXist-db transaction
     */
    private void setOrigin(final Txn transaction) {
            transaction.setOriginId(this.getClass().getName());
    }

    /**
     * Release lock on collection
     *
     * @param collection The collection, null is allowed
     * @param lockMode The lock mode.
     */
    private void releaseLock(final Collection collection, final int lockMode){
        if (collection != null) {
            collection.release(lockMode);
        }
    }

    @Override
    public void onMessage(final Message msg) {

        // Start reporting
        report.start();

        try {
            // Detect if the sender of the incoming message is the receiver
            if (StringUtils.isNotEmpty(localID)) {
                final String remoteID = msg.getStringProperty(Constants.EXIST_INSTANCE_ID);
                if (localID.equals(remoteID)) {
                    LOG.info("Incoming JMS messsage was originally sent by this instance. Processing stopped.");
                    return; // TODO: throw exception? probably not because message does not need to be re-received
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
                    LOG.debug(em.getReport());
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
                        final String errorMessage = String.format("Unknown resource type %s", em.getResourceType());
                        LOG.error(errorMessage);
                        throw new MessageReceiveException(errorMessage);
                }
                report.incMessageCounterOK();

            } else {
                // Only ByteMessage objects supported. 
                throw new MessageReceiveException(String.format("Could not handle message type %s", msg.getClass().getSimpleName()));
            }

        } catch (final MessageReceiveException ex) {
            // Thrown by local code. Just make it pass\
            report.addListenerError(ex);
            LOG.error(String.format("Could not handle received message: %s", ex.getMessage()), ex);
            throw ex;

        } catch (final Throwable t) {
            // Something really unexpected happened. Report
            report.addListenerError(t);
            LOG.error(t.getMessage(), t);
            throw new MessageReceiveException(String.format("Could not handle received message: %s", t.getMessage()), t);

        } finally {
            // update statistics
            report.stop();
            report.incMessageCounterTotal();
            report.addCumulatedProcessingTime();
        }
    }

    //
    // The code below handles the incoming message ; DW: should be moved to seperate class
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

        // Reference to the collection
        final Collection collection;

        // Get mime, or NULL when not available
        MimeType mime = MimeTable.getInstance().getContentTypeFor(docURI.toString());
        if (mime == null) {
            mime = MimeType.BINARY_TYPE;
        }

        // Get OWNER and Group
        final String userName = getUserName(metaData);
        final String groupName = getGroupName(metaData);

        // Get MIME_TYPE
        final String mimeType = getMimeType(metaData, mime.getName());

        // Get MODE
        final Integer mode = getMode(metaData);

        // Check for collection, create if not existent
        try {
            collection = getOrCreateCollection(colURI, userName, groupName, Permission.DEFAULT_COLLECTION_PERM);

        } catch (final MessageReceiveException e) {
            LOG.error(e.getMessage());
            throw e;
        }

        catch (final Throwable t) {
            if (LOG.isDebugEnabled()) {
                LOG.error(t.getMessage(), t);
            } else {
                LOG.error(t.getMessage());
            }
            throw new MessageReceiveException(String.format("Unable to create collection in database: %s", t.getMessage()));
        }


        try (DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             Txn txn = txnManager.beginTransaction()) {
            setOrigin(txn);

            final DocumentImpl doc;
            if (mime.isXMLType()) {

                // Stream into database
                final VirtualTempFile vtf = new VirtualTempFile(em.getPayload());
                final VirtualTempFileInputSource vt = new VirtualTempFileInputSource(vtf);
                final InputStream byteInputStream = vt.getByteStream();

                // DW: future improvement: determine compression based on property.
                GZIPInputStream gis = new GZIPInputStream(byteInputStream);
                InputSource inputsource = new InputSource(gis);

                // DW: collection can be null?
                final IndexInfo info = collection.validateXMLResource(txn, broker, docURI, inputsource);
                doc = info.getDocument();
                doc.getMetadata().setMimeType(mimeType);

                // reconstruct gzip input stream
                byteInputStream.reset();
                gis = new GZIPInputStream(byteInputStream);
                inputsource = new InputSource(gis);

                collection.store(txn, broker, info, inputsource, false);
                inputsource.getByteStream().close();

            } else {

                // Stream into database
                byte[] payload = em.getPayload();

                try (ByteArrayInputStream bais = new ByteArrayInputStream(payload);
                     GZIPInputStream gis = new GZIPInputStream(bais);
                     BufferedInputStream bis = new BufferedInputStream(gis)) {
                    // DW: collection can be null
                    doc = collection.addBinaryResource(txn, broker, docURI, bis, mimeType, payload.length);
                }
            }

            // Set owner,group and permissions
            final Permission permission = doc.getPermissions();
            if (userName != null) {
                permission.setOwner(userName);
            }
            if (groupName != null) {
                permission.setGroup(groupName);
            }
            if (mode != null) {
                permission.setMode(mode);
            }

            // Commit change
            txn.commit();


        } catch (final Throwable ex) {

            if (LOG.isDebugEnabled()) {
                LOG.error(ex.getMessage(), ex);
            } else {
                LOG.error(ex.getMessage());
            }

            throw new MessageReceiveException(String.format("Unable to write document into database: %s", ex.getMessage()));


        } finally {

            releaseLock(collection, Lock.WRITE_LOCK);

        }
    }

    /**
     * Metadata is updated in database
     *
     * TODO not usable yet
     */
    private void updateMetadataDocument(final eXistMessage em) {
        // Permissions
        // Mimetype
        // owner/groupname

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());
        final XmldbURI colURI = sourcePath.removeLastSegment();
        final XmldbURI docURI = sourcePath.lastSegment();

        // Get mime, or NULL when not available
        MimeType mime = MimeTable.getInstance().getContentTypeFor(docURI.toString());
        if (mime == null) {
            mime = MimeType.BINARY_TYPE;
        }


        // References to the database
        Collection collection = null;
        final DocumentImpl resource;

        try (DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             Txn txn = txnManager.beginTransaction()) {

            setOrigin(txn);

            // Open collection if possible, else abort
            collection = broker.openCollection(colURI, Lock.WRITE_LOCK);
            if (collection == null) {
                LOG.error(String.format("Collection does not exist %s", colURI));
                txn.abort();
                return; // be silent
            }

            // Open document if possible, else abort
            resource = collection.getDocument(broker, docURI);
            if (resource == null) {
                LOG.error(String.format("No resource found for path: %s", sourcePath));
                txn.abort();
                return; // be silent
            }

            // Get supplied metadata
            final Map<String, Object> metaData = em.getMetadata();

            final Permission perms = resource.getPermissions();

            final String userName = getUserName(metaData);
            if (userName != null) {
                perms.setGroup(userName);
            }

            final String groupName = getGroupName(metaData);
            if (groupName != null) {
                perms.setGroup(groupName);
            }

            final Integer mode = getMode(metaData);
            if (mode != null) {
                perms.setMode(mode);
            }

            final String mimeType = getMimeType(metaData, mime.getName());
            if (mimeType != null) {
                resource.getMetadata().setMimeType(mimeType);
            }

            // Commit change
            txn.commit();

        } catch (final Throwable e) {
            LOG.error(e);
            throw new MessageReceiveException(e.getMessage(), e);

        } finally {
            releaseLock(collection, Lock.WRITE_LOCK);
        }

    }

    /**
     * Remove document from database. If a document or collection does not exist, this is logged.
     */
    private void deleteDocument(final eXistMessage em) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());
        final XmldbURI colURI = sourcePath.removeLastSegment();
        final XmldbURI docURI = sourcePath.lastSegment();

        // Reference to the collection
        Collection collection = null;

        try (final DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
            Txn txn = txnManager.beginTransaction()) {

            setOrigin(txn);

            // Open collection if possible, else abort
            collection = broker.openCollection(colURI, Lock.WRITE_LOCK);
            if (collection == null) {
                final String errorText = String.format("Collection does not exist %s", colURI);
                LOG.error(errorText);
                txn.abort();
                return; // silently ignore
            }

            // Open document if possible, else abort
            final DocumentImpl resource = collection.getDocument(broker, docURI);
            if (resource == null) {
                LOG.error(String.format("No resource found for path: %s", sourcePath));
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

            throw new MessageReceiveException(t.getMessage(), t);

        } finally {
            releaseLock(collection, Lock.WRITE_LOCK);
        }
    }

    /**
     * Remove collection from database
     */
    private void deleteCollection(final eXistMessage em) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());

        Collection collection = null;

        try (DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             Txn txn = txnManager.beginTransaction()) {

            setOrigin(txn);

            // Open collection if possible, else abort
            collection = broker.openCollection(sourcePath, Lock.WRITE_LOCK);
            if (collection == null) {
                LOG.error(String.format("Collection does not exist: %s", sourcePath));
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

            throw new MessageReceiveException(t.getMessage());

        } finally {
            releaseLock(collection, Lock.WRITE_LOCK);
        }
    }

    /**
     * Created collection in database
     */
    private void createCollection(final eXistMessage em) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());

        final Map<String, Object> metaData = em.getMetadata();

        // Get OWNER/GROUP/MODE
        final String userName = getUserName(metaData);
        final String groupName = getGroupName(metaData);
        final Integer mode = getMode(metaData);

        getOrCreateCollection(sourcePath, userName, groupName, mode);
    }

    private Collection getOrCreateCollection(final XmldbURI sourcePath, final String userName, final String groupName, final Integer mode) throws MessageReceiveException {

        // Check if collection is already there ; do not modify
        try (DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()))) {
            final Collection collection = broker.openCollection(sourcePath, Lock.READ_LOCK);
            if (collection != null) {
                LOG.debug(String.format("Collection %s already exists", sourcePath));
                releaseLock(collection, Lock.READ_LOCK);
                return collection; // Just return the already existent collection
            }

        } catch (final Throwable e) {
            if (LOG.isDebugEnabled()) {
                LOG.error(e.getMessage(), e);
            } else {
                LOG.error(e.getMessage());
            }

            throw new MessageReceiveException(e.getMessage(), e);
        }

        // Reference to newly created collection
        Collection newCollection = null;

        // New collection to be created
        try (DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             Txn txn = txnManager.beginTransaction()) {

            setOrigin(txn);

            // Create collection
            newCollection = broker.getOrCreateCollection(txn, sourcePath);

            // Set owner,group and permissions
            final Permission permission = newCollection.getPermissions();
            if (userName != null) {
                permission.setOwner(userName);
            }
            if (groupName != null) {
                permission.setGroup(groupName);
            }
            if (mode != null) {
                permission.setMode(mode);
            }

            broker.saveCollection(txn, newCollection);

            // Commit change
            txn.commit();

        } catch (final Throwable t) {

            if (LOG.isDebugEnabled()) {
                LOG.error(t.getMessage(), t);
            } else {
                LOG.error(t.getMessage());
            }

            throw new MessageReceiveException(t.getMessage(), t);

        } finally {
            releaseLock(newCollection, Lock.WRITE_LOCK);
        }
        return newCollection;
    }

    private void relocateDocument(final eXistMessage em, final boolean keepDocument) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());
        final XmldbURI sourceColURI = sourcePath.removeLastSegment();
        final XmldbURI sourceDocURI = sourcePath.lastSegment();

        final XmldbURI destPath = XmldbURI.create(em.getDestinationPath());
        final XmldbURI destColURI = destPath.removeLastSegment();
        final XmldbURI destDocURI = destPath.lastSegment();

        Collection srcCollection = null;
        final DocumentImpl srcDocument;

        Collection destCollection = null;

        // Use the correct lock
        final int lockTypeOriginal = keepDocument ? Lock.READ_LOCK : Lock.WRITE_LOCK;

        try (DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             Txn txn = txnManager.beginTransaction()) {

            setOrigin(txn);

            // Open collection if possible, else abort
            srcCollection = broker.openCollection(sourceColURI, lockTypeOriginal);
            if (srcCollection == null) {
                LOG.error(String.format("Collection not found: %s", sourceColURI));
                txn.abort();
                return; // be silent
            }

            // Open document if possible, else abort
            srcDocument = srcCollection.getDocument(broker, sourceDocURI);
            if (srcDocument == null) {
                LOG.error(String.format("No resource found for path: %s", sourcePath));
                txn.abort();
                return; // be silent
            }

            // Open collection if possible, else abort
            destCollection = broker.openCollection(destColURI, Lock.WRITE_LOCK);
            if (destCollection == null) {
                LOG.error(String.format("Destination collection %s does not exist.", destColURI));
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
            LOG.error(e);
            throw new MessageReceiveException(e.getMessage(), e);

        } finally {
            releaseLock(destCollection, Lock.WRITE_LOCK);
            releaseLock(srcCollection, lockTypeOriginal);
        }
    }

    private void relocateCollection(final eXistMessage em, final boolean keepCollection) {

        final XmldbURI sourcePath = XmldbURI.create(em.getResourcePath());

        final XmldbURI destPath = XmldbURI.create(em.getDestinationPath());
        final XmldbURI destColURI = destPath.removeLastSegment();
        final XmldbURI destDocURI = destPath.lastSegment();

        Collection srcCollection = null;
        Collection destCollection = null;

        try (DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             Txn txn = txnManager.beginTransaction()) {

            setOrigin(txn);

            // Open collection if possible, else abort
            srcCollection = broker.openCollection(sourcePath, Lock.WRITE_LOCK);
            if (srcCollection == null) {
                LOG.error(String.format("Collection %s does not exist.", sourcePath));
                txn.abort();
                return; // be silent
            }

            // Open collection if possible, else abort
            destCollection = broker.openCollection(destColURI, Lock.WRITE_LOCK);
            if (destCollection == null) {
                LOG.error(String.format("Destination collection %s does not exist.", destColURI));
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
            LOG.error(e);
            throw new MessageReceiveException(e.getMessage());

        } finally {
            releaseLock(destCollection, Lock.WRITE_LOCK);
            releaseLock(srcCollection, Lock.WRITE_LOCK);
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
        final String userName = getUserName(metaData);
        final String groupName = getGroupName(metaData);
        final Integer mode = getMode(metaData);


        Collection collection = null;

        try( DBBroker broker = brokerPool.get(Optional.of(securityManager.getSystemSubject()));
             Txn txn = txnManager.beginTransaction()) {

            setOrigin(txn);

            // Open collection if possible, else abort
            collection = broker.openCollection(sourceColURI, Lock.WRITE_LOCK);
            if (collection == null) {
                LOG.error(String.format("Collection not found: %s", sourceColURI));
                txnManager.abort(txn);
                return; // be silent
            }

            final Permission permission = collection.getPermissions();
            if (userName != null) {
                permission.setOwner(userName);
            }
            if (groupName != null) {
                permission.setGroup(groupName);
            }
            if (mode != null) {
                permission.setMode(mode);
            }

            // Commit change
            txn.commit();

        } catch (final Throwable e) {
            LOG.error(e);
            throw new MessageReceiveException(e.getMessage());

        } finally {
            releaseLock(collection,Lock.WRITE_LOCK );

        }

    }


    /**
     * Get valid username for database, else system subject if not valid.
     */
    private String getUserName(final Map<String, Object> metaData) {

        String userName = null;
        final Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_OWNER);
        if (prop != null && prop instanceof String) {
            userName = (String) prop;
        } else {
            LOG.debug("No username provided");
            return userName;
        }

        Account account = securityManager.getAccount(userName);
        if (account == null) {
            final String errorText = String.format("Username %s does not exist.", userName);
            LOG.error(errorText);

            account = securityManager.getSystemSubject();
            userName = account.getName();
        }

        return userName;
    }

    /**
     * Get valid groupname for database for database, else system subject if not existent
     */
    private String getGroupName(final Map<String, Object> metaData) {

        String groupName = null;
        final Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_GROUP);
        if (prop != null && prop instanceof String) {
            groupName = (String) prop;
        } else {
            LOG.debug("No groupname provided");
            return groupName;
        }

        Group group = securityManager.getGroup(groupName);
        if (group == null) {
            final String errorText = String.format("Group %s does not exist.", groupName);
            LOG.error(errorText);

            group = securityManager.getSystemSubject().getDefaultGroup();
            groupName = group.getName();
        }

        return groupName;
    }

    private String getMimeType(Map<String, Object> metaData, String defaultName) {

        String mimeType = null;
        if (prop != null && prop instanceof String) {
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

    private Integer getMode(final Map<String, Object> metaData) {
        // Get/Set permissions
        Integer mode = null;
        final Object prop = metaData.get(MessageHelper.EXIST_RESOURCE_MODE);
        if (prop != null && prop instanceof Integer) {
            mode = (Integer) prop;
        } else {
            LOG.debug("No mode provided");
            return mode;
        }
        return mode;
    }

   

}
