(:
 : Example: send 10 JMS messages with a few message properties
 :)
xquery version "3.0";

import module namespace jms="http://exist-db.org/xquery/messaging" 
              at "java:org.exist.jms.xquery.MessagingModule";

(: Configuration for setting-up an JMS connection :)
let $jmsConfiguration :=
    map {
        "java.naming.factory.initial" 
            := "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        "java.naming.provider.url" := "tcp://miniserver.local:61616",
        "destination" := "dynamicQueues/eXistdb-test",
        "connection-factory" := "ConnectionFactory",
        "exist.connection.pool" := "yes"
    }
 



return
    
    for $i in (1 to 4)
    
    (: JMS message properties :)
    let $messageProperties :=
    map {
        "ID" := $i   
    }
    
    (: The actual message payload :)
    let $content := <data>{$i}</data>
    
    return
    
        (: Send message to the JMS broker :)
        jms:send( $content , $messageProperties, $jmsConfiguration ) 
      