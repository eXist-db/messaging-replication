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
package org.exist.jms.messaging.shared;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.exist.util.ConfigurationHelper;

/**
 * Helper class to obtain a unique identifier for this eXist-db / JMS instance
 * 
 * @author Dannes Wessels
 */
public class Identity {

    private final static Logger LOG = Logger.getLogger(Identity.class);

    private static Identity instance = null;
    private File identityFile = null;
    private String identity = null;

    private Identity() {
        findIdentityFile();
        getIdentityFromFile();
    }
    
    public static Identity getInstance(){
        if(instance==null){
            instance = new Identity();
        }
        return instance;
    }
    
    public String getIdentity(){
        return identity;
    }

    /**
     * Find identity file
     */
    private void findIdentityFile() {

        if (identityFile == null) {
            File existHome = ConfigurationHelper.getExistHome();
            File dataDir = new File(existHome, "webapp/WEB-INF/data");
            identityFile = (dataDir.exists()) ? new File(dataDir, "jms.identity") : new File(existHome, "jms.identity");
        }
    }

    /**
     * Read identity from file, create if not existent
     */
    private void getIdentityFromFile() {

        Properties props = new Properties();

        // Read if possible
        if (identityFile.exists()) {
            InputStream is = null;
            try {
                is = new FileInputStream(identityFile);
                props.load(is);
                
                identity = props.getProperty("identity");
            } catch (IOException ex) {
                LOG.error(ex.getMessage());
            } finally {
                IOUtils.closeQuietly(is);
            }

        }

        // Create and write when needed
        if (!identityFile.exists() || identity == null) {
            identity = UUID.randomUUID().toString();

            props.setProperty("identity", identity);

            OutputStream os = null;
            try {
                os = new FileOutputStream(identityFile);
                props.store(os, "");
                
            } catch (IOException ex) {
                LOG.error(ex.getMessage());
            } finally {
                IOUtils.closeQuietly(os);
            }

        }

    }
}
