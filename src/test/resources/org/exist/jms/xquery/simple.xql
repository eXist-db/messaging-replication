xquery version "3.0";

module namespace m = "http://foo.org/xquery/jms/sometest";

declare namespace test = "http://exist-db.org/xquery/xqsuite";

import module namespace messaging = "http://exist-db.org/xquery/messaging" at "java:org.exist.jms.xquery.MessagingModule";

declare variable $m:jmsConfiguration := map {
"java.naming.factory.initial" : "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
"java.naming.provider.url" : "tcp://localhost:61616",
"destination" : "dynamicQueues/eXistdb-messaging-example",
"connection-factory" : "ConnectionFactory"
};

declare variable $m:messageProperties := map {
"Su" : "Sunday",
"Mo" : xs:integer(1),
"Tu" : 2,
"We" : true(),
2 : "a",
"a" : xs:short(5)
};


declare
%test:assertEquals("tcp://localhost:61616")
function m:testSimple() as xs:string {


(: Send message to the JMS broker :)
    messaging:send(<data>{util:uuid()}</data>, $m:messageProperties, $m:jmsConfiguration)//java.naming.provider.url
};