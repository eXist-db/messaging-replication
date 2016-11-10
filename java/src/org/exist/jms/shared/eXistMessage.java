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
package org.exist.jms.shared;

import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.jms.JMSException;
import javax.jms.Message;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Container class for clustering messages.
 *
 * @author Dannes Wessels
 */
public class eXistMessage {

    /**
     * Header to describe operation on resource, e.g. CREATE UPDATE
     */
    public final static String EXIST_RESOURCE_OPERATION = "exist.resource.operation";

    /**
     * Header to describe resource, DOCUMENT or COLLECTION
     */
    public final static String EXIST_RESOURCE_TYPE = "exist.resource.type";

    /**
     * Header to describe path of resource
     */
    public final static String EXIST_SOURCE_PATH = "exist.source.path";

    /**
     * Header to describe destination path, for COPY and MOVE operation
     */
    public final static String EXIST_DESTINATION_PATH = "exist.destination.path";

    private ResourceOperation resourceOperation = ResourceOperation.UNDEFINED;
    private ResourceType resourceType = ResourceType.UNDEFINED;
    private ContentType contentType = ContentType.UNDEFINED;

    private String path;
    private String destination;
    private byte[] payload;

    private Map<String, Object> metaData = new HashMap<>();

    public void setResourceOperation(final ResourceOperation type) {
        resourceOperation = type;
    }

    public ResourceOperation getResourceOperation() {
        return resourceOperation;
    }

    /**
     * Set resource operation
     *
     * @param type Name of resource operation
     * @throws IllegalArgumentException When argument cannot be converted to enum value.
     */
    public void setResourceOperation(final String type) {
        resourceOperation = ResourceOperation.valueOf(type.toUpperCase(Locale.ENGLISH));
    }

    public void setResourceType(final ResourceType type) {
        resourceType = type;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    /**
     * Set Resource type.
     *
     * @param type resource type.
     * @throws IllegalArgumentException When argument cannot be converted to enum value.
     */
    public void setResourceType(final String type) {
        resourceType = eXistMessage.ResourceType.valueOf(type.toUpperCase(Locale.ENGLISH));
    }

    public String getResourcePath() {
        return path;
    }

    public void setResourcePath(final String path) {
        this.path = path;
    }

    public String getDestinationPath() {
        return destination;
    }

    public void setDestinationPath(final String path) {
        destination = path;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(final byte[] data) {
        payload = data;
    }

    public void resetPayload() {
        payload = new byte[0];
    }

    public Map<String, Object> getMetadata() {
        return metaData;
    }

    public void setMetadata(final Map<String, Object> props) {
        metaData = props;
    }

    public ContentType getDocumentType() {
        return contentType;
    }

    public void setDocumentType(final ContentType documentType) {
        this.contentType = documentType;
    }

    /**
     * Get one-liner report of message, including the JMS properties.
     *
     * @return Report of message
     */
    public String getFullReport() {
        final StringBuilder sb = new StringBuilder();

        sb.append("Message summary: ");
        sb.append("ResourceType='").append(resourceType.toString()).append("'  ");
        sb.append("ResourceOperation='").append(resourceOperation).append("'  ");

        sb.append("ResourcePath='").append(path).append("'  ");

        if (destination != null) {
            sb.append("DestinationPath='").append(resourceType.toString()).append("'  ");
        }

        if (payload != null && payload.length > 0) {
            sb.append("PayloadSize='").append(payload.length).append("'  ");
        }

        // Iterate over properties if present
        final Set<String> keys = metaData.keySet();
        if (!keys.isEmpty()) {
            sb.append("###  ");

            keys.forEach((key) -> {
                final Object val = metaData.get(key);
                if (val != null) {
                    sb.append(key).append("='").append(val.toString()).append("'  ");
                }
            });
        }

        return sb.toString();
    }

    public String getReport() {
        return "type='" + resourceType.toString() + "' " +
                "resource='" + path + "' " +
                "operation='" + resourceOperation + "'";
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    /**
     * Update JMS message properties with replication details.
     *
     * @param message JMS message.
     * @throws JMSException A property could not be set.
     */
    public void updateMessageProperties(final Message message) throws JMSException {

        // Set eXist-db clustering specific details
        message.setStringProperty(eXistMessage.EXIST_RESOURCE_OPERATION, getResourceOperation().name());
        message.setStringProperty(eXistMessage.EXIST_RESOURCE_TYPE, getResourceType().name());
        message.setStringProperty(eXistMessage.EXIST_SOURCE_PATH, getResourcePath());

        if (getDestinationPath() != null) {
            message.setStringProperty(eXistMessage.EXIST_DESTINATION_PATH, getDestinationPath());
        }

//        // Retrieve and set JMS identifier
//        String id = Identity.getInstance().getIdentity();
//        if (id != null) {
//            message.setStringProperty(Constants.EXIST_INSTANCE_ID, id);
//        }

        // Set other details
        final Map<String, Object> metaDataMap = getMetadata();

        for (final Map.Entry<String, Object> entry : metaDataMap.entrySet()) {

            final String item = entry.getKey();
            final Object value = entry.getValue();

            if (value instanceof String) {
                message.setStringProperty(item, (String) value);

            } else if (value instanceof Integer) {
                message.setIntProperty(item, (Integer) value);

            } else if (value instanceof Long) {
                message.setLongProperty(item, (Long) value);

            } else {
                message.setStringProperty(item, "" + value);
            }
        }
    }

    /**
     * Atomic operations on resources
     */
    public enum ResourceOperation {
        CREATE, UPDATE, DELETE, MOVE, COPY, METADATA, UNDEFINED
    }

    /**
     * Types of exist-db resources
     */
    public enum ResourceType {
        DOCUMENT, COLLECTION, UNDEFINED
    }

    /**
     * Types of exist-db resources
     */
    public enum ContentType {
        XML, BINARY, UNDEFINED
    }
}
