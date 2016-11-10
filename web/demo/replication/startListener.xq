xquery version "3.1";

(:~
: Simple script to start the replication listener.
:
: To have this script executed when eXist-db is started:
:
: - enable in conf.xml the XQueryStartupTrigger
: - carefully read the instructions about permissons
: - copy this script into /db/system/autostart
: - restart eXist-db, check logs for messages from the XQueryStartupTrigger
:)

import module namespace replication="http://exist-db.org/xquery/replication" at "java:org.exist.jms.xquery.ReplicationModule";

let $jmsConfiguration := map {
        "java.naming.factory.initial" := "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        "java.naming.provider.url" := "tcp://localhost:61616",
        "connection-factory" := "ConnectionFactory",
        "destination" := "dynamicTopics/eXistdb-replication-example",
        "subscriber.name" := "SubscriptionId",
        "connection.client-id" := "ClientId"
    }

return
    replication:register($jmsConfiguration)