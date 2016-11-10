xquery version "3.0";

(:~
:  Example to send a message with an XML payload and additional message properties
:)

import module namespace messaging="http://exist-db.org/xquery/messaging" at "java:org.exist.jms.xquery.MessagingModule";

(: Configuration for setting-up an JMS connection :)
let $jmsConfiguration := map {
    "java.naming.factory.initial" := "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    "java.naming.provider.url" := "tcp://localhost:61616",
    "destination" := "dynamicQueues/eXistdb-messaging-demo",
    "connection-factory" := "ConnectionFactory"
}

(: JMS message properties :)
let $messageProperties := map {
    "Su" := "Sunday",
    "Mo" := xs:integer(1),
    "Tu" := 2,
    "We" := true(),
    2 := "a",
    "a" := xs:short(5)
}


return

    (: Send message to the JMS broker :)
    messaging:send( <data>{util:uuid()}</data> , $messageProperties, $jmsConfiguration )

