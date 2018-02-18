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
package org.exist.replication.jms.obsolete;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.jms.shared.eXistMessage;

import javax.jms.*;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;


/**
 * Listener for actual handling of JMS message.
 *
 * @author Dannes Wessels
 */
public class FileSystemListener implements MessageListener {

    private final static Logger LOG = LogManager.getLogger();

    private static File baseDir;

    public FileSystemListener() {
        baseDir = new File("clusteringTest");
        if (!baseDir.exists()) {
            LOG.info("Creating " + baseDir.getAbsolutePath());
            baseDir.mkdirs();
        }
    }

    private eXistMessage convertMessage(final BytesMessage bm) {
        final eXistMessage em = new eXistMessage();

        try {
            final Enumeration e = bm.getPropertyNames();
            while (e.hasMoreElements()) {
                final Object next = e.nextElement();
                if (next instanceof String) {
                    em.getMetadata().put((String) next, bm.getObjectProperty((String) next));
                }
            }

            String value = bm.getStringProperty(eXistMessage.EXIST_RESOURCE_TYPE);
            final eXistMessage.ResourceType resourceType = eXistMessage.ResourceType.valueOf(value);
            em.setResourceType(resourceType);

            value = bm.getStringProperty(eXistMessage.EXIST_RESOURCE_OPERATION);
            final eXistMessage.ResourceOperation changeType = eXistMessage.ResourceOperation.valueOf(value);
            em.setResourceOperation(changeType);

            value = bm.getStringProperty(eXistMessage.EXIST_SOURCE_PATH);
            em.setResourcePath(value);

            value = bm.getStringProperty(eXistMessage.EXIST_DESTINATION_PATH);
            em.setDestinationPath(value);

            final long size = bm.getBodyLength();
            LOG.debug("actual length=" + size);

            // This is potentially memory intensive
            final byte[] payload = new byte[(int) size];
            bm.readBytes(payload);
            em.setPayload(payload);

        } catch (final JMSException ex) {
            LOG.error(ex);
        }

        return em;

    }

    @Override
    public void onMessage(final Message message) {
        try {
            LOG.info("JMSMessageID=" + message.getJMSMessageID());

            final StringBuilder sb = new StringBuilder();

            // Write properties
            final Enumeration names = message.getPropertyNames();
            for (final Enumeration<?> e = names; e.hasMoreElements(); ) {
                final String key = (String) e.nextElement();
                sb.append("'").append(key).append("='").append(message.getStringProperty(key)).append("' ");
            }
            LOG.info(sb.toString());

            // Handle message
            if (message instanceof TextMessage) {
                LOG.info(((TextMessage) message).getText());

            } else if (message instanceof BytesMessage) {

                final BytesMessage bm = (BytesMessage) message;

                final eXistMessage em = convertMessage(bm);


                switch (em.getResourceType()) {
                    case DOCUMENT:
                        handleDocument(em);
                        break;
                    case COLLECTION:
                        handleCollection(em);
                        break;
                    default:
                        LOG.error("Unknown resource type");
                        break;
                }

            }

        } catch (final JMSException ex) {
            LOG.error(ex);
        }

    }

    private void handleDocument(final eXistMessage em) {

        LOG.info(em.getFullReport());

        // Get original path
        final String resourcePath = em.getResourcePath();

        final String[] srcSplitPath = splitPath(resourcePath);
        final String srcDir = srcSplitPath[0];
        final String srcDoc = srcSplitPath[1];


        final File dir = new File(baseDir, srcDir);
        final File file = new File(dir, srcDoc);

        switch (em.getResourceOperation()) {
            case CREATE:
            case UPDATE:
                // Create dirs if not existent

                dir.mkdirs();

                // Create file reference

                LOG.info(file.getAbsolutePath());

                try {
                    // Prepare streams
                    final FileOutputStream fos = new FileOutputStream(file);
                    final ByteArrayInputStream bais = new ByteArrayInputStream(em.getPayload());
                    final GZIPInputStream gis = new GZIPInputStream(bais);

                    // Copy and unzip
                    IOUtils.copy(gis, fos);

                    // Cleanup
                    IOUtils.closeQuietly(fos);
                    IOUtils.closeQuietly(gis);
                } catch (final IOException ex) {
                    LOG.error(ex);

                }
                break;

            case DELETE:
                FileUtils.deleteQuietly(file);
                break;

            case MOVE:
                final File mvFile = new File(baseDir, em.getDestinationPath());
                try {
                    FileUtils.moveFile(file, mvFile);
                } catch (final IOException ex) {
                    LOG.error(ex);
                }
                break;

            case COPY:
                final File cpFile = new File(baseDir, em.getDestinationPath());
                try {
                    FileUtils.copyFile(file, cpFile);
                } catch (final IOException ex) {
                    LOG.error(ex);
                }
                break;

            default:
                LOG.error("Unknown change type");
        }
    }

    private String[] splitPath(final String fullPath) {
        final String directory;
        final String documentname;
        final int separator = fullPath.lastIndexOf("/");
        if (separator == -1) {
            directory = "";
            documentname = fullPath;
        } else {
            directory = fullPath.substring(0, separator);
            documentname = fullPath.substring(separator + 1);
        }

        return new String[]{directory, documentname};
    }

    private void handleCollection(final eXistMessage em) {

        final File src = new File(baseDir, em.getResourcePath());


        switch (em.getResourceOperation()) {
            case CREATE:
            case UPDATE:
                try {
                    // Create dirs if not existent
                    FileUtils.forceMkdir(src);
                } catch (final IOException ex) {
                    LOG.error(ex);
                }

                break;

            case DELETE:
                FileUtils.deleteQuietly(src);
                break;

            case MOVE:
                final File mvDest = new File(baseDir, em.getDestinationPath());
                try {
                    FileUtils.moveDirectoryToDirectory(src, mvDest, true);
                } catch (final IOException ex) {
                    LOG.error(ex);
                }
                break;

            case COPY:

                final File cpDest = new File(baseDir, em.getDestinationPath());
                try {
                    FileUtils.copyDirectoryToDirectory(src, cpDest);
                } catch (final IOException ex) {
                    LOG.error(ex);
                }
                break;

            default:
                LOG.error("Unknown change type");
        }
    }
}
