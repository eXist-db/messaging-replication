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
package org.exist.jms.replication.shared;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.dom.persistent.BinaryDocument;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.persistent.DocumentMetadata;
import org.exist.jms.shared.eXistMessage;
import org.exist.security.Permission;
import org.exist.storage.DBBroker;
import org.exist.storage.serializers.Serializer;
import org.exist.storage.txn.Txn;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Helper class for retrieving (meta)data from an in eXist stored document.
 *
 * @author Dannes Wessels
 */
@SuppressWarnings("UnusedAssignment")
public class MessageHelper {

    public static final String EXIST_RESOURCE_CONTENTLENGTH = "exist.resource.contentlength";
    public static final String EXIST_RESOURCE_DOCUMENTID = "exist.resource.documentid";
    public static final String EXIST_RESOURCE_GROUP = "exist.resource.group";
    public static final String EXIST_RESOURCE_MIMETYPE = "exist.resource.mimetype";
    public static final String EXIST_RESOURCE_LASTMODIFIED = "exist.resource.lastmodified";
    public static final String EXIST_RESOURCE_CREATIONTIME = "exist.resource.creationtime";
    public static final String EXIST_RESOURCE_OWNER = "exist.resource.owner";
    public static final String EXIST_RESOURCE_TYPE = "exist.resource.type";
    public static final String EXIST_RESOURCE_MODE = "exist.resource.permission.mode";
    public static final String EXIST_MESSAGE_CONTENTENCODING = "exist.message.content-encoding";

    private final static Logger LOG = LogManager.getLogger(MessageHelper.class);


    /**
     * Serialize document to byte array as gzipped document.
     *
     * @param broker   The broker
     * @param document Document to compress
     * @return document as array of bytes
     * @throws IOException When the
     */
    public static byte[] gzipSerialize(final DBBroker broker, final Txn transaction, final DocumentImpl document) throws IOException {

        // This is the weak spot, the data is serialized into
        // a byte array. Better to have an overflow to a file,
        byte[] payload;

        if (document.getResourceType() == DocumentImpl.XML_FILE) {

            // Stream XML document
            final Serializer serializer = broker.getSerializer();

            try {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();

                try (final GZIPOutputStream gos = new GZIPOutputStream(baos);
                     final Writer w = new OutputStreamWriter(gos, StandardCharsets.UTF_8)) {
                    serializer.serialize(document, w);
                    w.flush();
                }

                payload = baos.toByteArray();


            } catch (final SAXException | IOException e) {
                payload = new byte[0];
                LOG.error(e);
                throw new IOException(String.format("Error while serializing XML document: %s", e.getMessage()), e);

            } catch (final Throwable e) {
                payload = new byte[0];
                System.gc(); // recover from out of memory exception
                LOG.error(e.getMessage(), e);
                throw new IOException(String.format("Error while serializing XML document: %s", e.getMessage()), e);
            }

        } else {
            // Stream NON-XML document

            try {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();

                try (final GZIPOutputStream gos = new GZIPOutputStream(baos)) {
                    // DW: check classtype before using
                    broker.readBinaryResource(transaction, (BinaryDocument) document, gos);
                    gos.flush();
                }

                payload = baos.toByteArray();

            } catch (final IOException e) {
                payload = new byte[0];
                LOG.error(e);
                throw new IOException(String.format("Error while serializing binary document: %s", e.getMessage()), e);

            } catch (final Throwable e) {
                payload = new byte[0];
                System.gc(); // recover from out of memory exception
                LOG.error(e.getMessage(), e);
                throw new IOException(String.format("Error while serializing binary document: %s", e.getMessage()), e);
            }
        }


        return payload;

    }

    public static void retrieveDocMetadata(final Map<String, Object> props, final DocumentMetadata docMetadata) {
        if (docMetadata == null) {
            LOG.error("no metadata supplied");

        } else {
            props.put(EXIST_RESOURCE_MIMETYPE, docMetadata.getMimeType());
            props.put(EXIST_RESOURCE_LASTMODIFIED, docMetadata.getLastModified());
            props.put(EXIST_RESOURCE_CREATIONTIME, docMetadata.getCreated());
        }
    }

    public static void retrievePermission(final Map<String, Object> props, final Permission perm) {
        if (perm == null) {
            LOG.error("no permissions supplied");

        } else {
            props.put(EXIST_RESOURCE_OWNER, perm.getOwner().getName());
            props.put(EXIST_RESOURCE_GROUP, perm.getGroup().getName());
            props.put(EXIST_RESOURCE_MODE, perm.getMode());
        }
    }


    public static void retrieveFromDocument(final Map<String, Object> props, final DocumentImpl document) {
        // We do not differ between DOCUMENT subtypes,
        // mime-type is set in document metadata EXIST_RESOURCE_MIMETYPE. /ljo
        props.put(EXIST_RESOURCE_TYPE, eXistMessage.ResourceType.DOCUMENT);
        props.put(EXIST_RESOURCE_DOCUMENTID, document.getDocId());
        props.put(EXIST_RESOURCE_CONTENTLENGTH, document.getContentLength());

    }

    public static void retrieveFromCollection(final Map<String, Object> props, final Collection collection) {
        props.put(EXIST_RESOURCE_CREATIONTIME, collection.getMetadata().getCreated());
    }
}
