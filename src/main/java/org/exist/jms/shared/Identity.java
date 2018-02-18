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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.util.ConfigurationHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

/**
 * Helper class to obtain a unique identifier for this eXist-db / JMS instance
 *
 * @author Dannes Wessels
 */
public class Identity {

    private final static Logger LOG = LogManager.getLogger(Identity.class);

    private static final String IDENTITY_PROP = "identity";

    private static Identity instance = null;
    private Path identityFile = null;
    private String identity = null;

    private Identity() {
        findIdentityFile();
        getIdentityFromFile();
    }

    public static synchronized Identity getInstance() {
        if (instance == null) {
            instance = new Identity();
        }
        return instance;
    }

    public String getIdentity() {
        return identity;
    }

    /**
     * Find identity file
     */
    private void findIdentityFile() {

        if (identityFile == null) {
            final Optional<Path> existHome = ConfigurationHelper.getExistHome();

            if (existHome.isPresent()) {
                final Path dataDir = existHome.get().resolve("webapp/WEB-INF/data");
                identityFile = (Files.exists(dataDir)) ? dataDir.resolve("jms.identity") : existHome.get().resolve("jms.identity");
            } else {
                LOG.error("eXist_home not found");
            }

        }
    }

    /**
     * Read identity from file, create if not existent
     */
    private void getIdentityFromFile() {

        final Properties props = new Properties();

        // Read if possible
        if (Files.exists(identityFile)) {

            LOG.info(String.format("Read jms identity from %s", identityFile.toString()));

            try {
                try (InputStream is = Files.newInputStream(identityFile)) {
                    props.load(is);
                    identity = props.getProperty(IDENTITY_PROP);
                }

            } catch (final IOException ex) {
                LOG.error(ex.getMessage());
            }

        }

        // Create and write when needed
        if (Files.notExists(identityFile) || identity == null) {

            LOG.info(String.format("Create new jms identity into %s", identityFile.toString()));

            identity = UUID.randomUUID().toString();
            props.setProperty(IDENTITY_PROP, identity);

            try {
                try (OutputStream os = Files.newOutputStream(identityFile)) {
                    props.store(os, "");
                }

            } catch (final IOException ex) {
                LOG.error(ex.getMessage());
            }

        }

    }


}
