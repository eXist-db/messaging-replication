xquery version "3.0";

import module namespace jms="http://exist-db.org/xquery/messaging" at "java:org.exist.jms.xquery.MessagingModule";

(: Configuration for setting-up an JMS connection :)
let $jmsConfiguration :=
    map {
        "java.naming.factory.initial" := "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        "java.naming.provider.url" := "tcp://myserver.local:61616",
        "destination" := "dynamicQueues/eXistdbTest",
        "connection-factory" := "ConnectionFactory"
    }
 
 (: JMS message properties :)
let $messageProperties :=
    map {
        "Su" := "Sunday",
        "Mo" := xs:integer(1),
        "Tu" := 2,
        "We" := true(),
        2 := "a",
        "test" := (1,2,3,4,5), (: not send :)
        "a" := xs:short(5)      
    }


return
    
    for $i in (1 to 10)
    (: The actual message payload :)
    let $content := <a>{$i}</a>
    return
    (: Send message to the JMS broker :)
    jms:send( $content , $messageProperties, $jmsConfiguration) 