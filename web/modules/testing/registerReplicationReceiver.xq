(: 
 : Example: register receiver with additional parameters 
 :)
xquery version "3.0";


import module namespace replication="http://exist-db.org/xquery/replication" 
                        at "java:org.exist.jms.xquery.ReplicationModule";


(: Configuration for setting-up the JMS connection :)
let $jmsConfiguration :=
    map {
        "java.naming.factory.initial" 
            := "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        "java.naming.provider.url" := "tcp://miniserver.local:61616",
        "destination" := "dynamicTopics/eXistdb-replication-demo",
        "connection-factory" := "ConnectionFactory",
        "subscriber.name" :="SubscriptionId",
        "connection.client-id" :="ClientId"
    }
    
    
return
    (: Register the function to the JMS broker :)
    replication:register($jmsConfiguration)
      