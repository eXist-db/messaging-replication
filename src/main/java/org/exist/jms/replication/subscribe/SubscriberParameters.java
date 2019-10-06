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
package org.exist.jms.replication.subscribe;

import org.apache.commons.lang3.StringUtils;
import org.exist.jms.replication.shared.ClientParameterException;
import org.exist.jms.replication.shared.ClientParameters;
import org.exist.jms.shared.Constants;

import javax.naming.Context;

/**
 * Subscriber  specific properties.
 *
 * @author Dannes Wessels dannes@exist-db.org
 */
public class SubscriberParameters extends ClientParameters {

    public static final String SUBSCRIBER_NAME = Constants.SUBSCRIBER_NAME; // "subscriber-name";
    public static final String MESSAGE_SELECTOR = Constants.MESSAGE_SELECTOR; //"messageselector";
    public static final String DURABLE = Constants.DURABLE; //"durable";
    public static final String NO_LOCAL = Constants.NO_LOCAL; //"nolocal";

    private String subscriberName;
    private String messageSelector;

    private boolean noLocal = Boolean.TRUE;
    private boolean durable = Boolean.TRUE;

    public boolean isDurable() {
        return durable;
    }

    public String getSubscriberName() {
        return subscriberName;
    }

    public String getMessageSelector() {
        return messageSelector;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    @Override
    public void processParameters() throws ClientParameterException {

        // Fill defaults when net set
        fillActiveMQbrokerDefaults();

        // java.naming.factory.initial
        String value = props.getProperty(Context.INITIAL_CONTEXT_FACTORY);
        initialContextFactory = value;

        // java.naming.provider.url
        value = props.getProperty(Context.PROVIDER_URL);
        providerUrl = value;

        // Connection factory
        value = props.getProperty(Constants.CONNECTION_FACTORY);
        if (StringUtils.isBlank(value)) {
            value = "ConnectionFactory";
            LOG.info("No {} set, using default value '{}'", Constants.CONNECTION_FACTORY, value);
        }
        connectionFactory = value;


        // Destination / topic
        value = props.getProperty(Constants.DESTINATION);
        if (StringUtils.isBlank(value)) {
            value = "dynamicTopics/eXistdb";
            LOG.info("No {} using default value '{}' which is suitable for activeMQ", Constants.DESTINATION, value);
        }
        topic = value;

        // Client ID ; 
        // for durable subscriptions later an additional check
        // is performed.
        value = props.getProperty(Constants.CLIENT_ID);
        if (StringUtils.isNotBlank(value)) {
            clientId = value;
            LOG.debug("{}: {}", Constants.CLIENT_ID, value);
        } else {
            LOG.debug( "{} is not set.", Constants.CLIENT_ID);
        }


        // Get subscribername
        value = props.getProperty(SUBSCRIBER_NAME);
        if (StringUtils.isBlank(value)) {
            final String errorText = "'" + SUBSCRIBER_NAME + "' is not set.";
            LOG.error(errorText);
            throw new ClientParameterException(errorText);
        }
        subscriberName = value;

        // Get messageSelector
        value = props.getProperty(MESSAGE_SELECTOR);
        if (value != null) {
            LOG.info("Message selector '{}'", messageSelector);
            messageSelector = value;
        }

        // Get NoLocal value, default no local copies
        value = props.getProperty(NO_LOCAL);
        if (value != null) {

            if ("FALSE".equalsIgnoreCase(value) || "NO".equalsIgnoreCase(value)) {
                noLocal = false;

            } else if ("TRUE".equalsIgnoreCase(value) || "YES".equalsIgnoreCase(value)) {
                noLocal = true;

            } else {
                final String errorText = "'" + NO_LOCAL + "' contains wrong value '" + value + "'";
                LOG.error(errorText);
                throw new ClientParameterException(errorText);
            }

        }

        // Get Durable value, 
        value = props.getProperty(DURABLE);
        if (value != null) {

            if ("FALSE".equalsIgnoreCase(value) || "NO".equalsIgnoreCase(value)) {
                durable = false;

            } else if ("TRUE".equalsIgnoreCase(value) || "YES".equalsIgnoreCase(value)) {
                durable = true;

            } else {
                final String errorText = "'" + DURABLE + "' contains wrong value '" + value + "'";
                LOG.error(errorText);
                throw new ClientParameterException(errorText);
            }
        }

        // FOr a durable connection (default) a clientId must be set
        if (durable && clientId == null) {
            final String errorText = "For durable connections the " + Constants.CLIENT_ID + " must be set.";
            LOG.error(errorText);
            throw new ClientParameterException(errorText);
        }

        // Get connection authentication
        connectionUsername = props.getProperty(Constants.JMS_CONNECTION_USERNAME);
        connectionPassword = props.getProperty(Constants.JMS_CONNECTION_PASSWORD);
    }

    @Override
    public String getReport() {
        return String.format("Subscriber configuration: %s='%s' %s='%s' %s='%s' %s='%s' %s='%s' %s='%s' %s='%s' %s='%s'",
                Context.INITIAL_CONTEXT_FACTORY, initialContextFactory, Context.PROVIDER_URL, providerUrl,
                Constants.DESTINATION, topic, Constants.CLIENT_ID, clientId, SUBSCRIBER_NAME, subscriberName,
                MESSAGE_SELECTOR, messageSelector, NO_LOCAL, noLocal, DURABLE, durable);
    }
}
