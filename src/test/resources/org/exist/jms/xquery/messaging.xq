xquery version "3.0";

module namespace m = "http://foo.org/xquery/jms/messaging";

declare namespace test = "http://exist-db.org/xquery/xqsuite";

import module namespace messaging = "http://exist-db.org/xquery/messaging" at "java:org.exist.jms.xquery.MessagingModule";
import module namespace jms = "http://exist-db.org/xquery/jms" at "java:org.exist.jms.xquery.JmsModule";

declare variable $m:jmsConfiguration := map {
"java.naming.factory.initial" : "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
"java.naming.provider.url" : "tcp://localhost:61616",
"destination" : "dynamicQueues/messagingTest",
"connection-factory" : "ConnectionFactory"
};

declare variable $m:messageProperties := map {
"Su" : "Sunday",
"Mo" : xs:integer(1)
};


declare
function m:pause()  {
    for $i in (1 to 100000) return jms:list()
};

declare
%test:assertEquals(0)
function m:message()  {

(: Send message to the JMS broker :)
let $send :=   messaging:send( <data>{util:uuid()}</data> , $m:messageProperties, $m:jmsConfiguration )

let $pause2 :=  m:pause()

return (0)
};