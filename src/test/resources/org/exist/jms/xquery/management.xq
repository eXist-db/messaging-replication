xquery version "3.0";

module namespace m = "http://foo.org/xquery/jms/management";

declare namespace test = "http://exist-db.org/xquery/xqsuite";

import module namespace messaging = "http://exist-db.org/xquery/messaging" at "java:org.exist.jms.xquery.MessagingModule";
import module namespace jms = "http://exist-db.org/xquery/jms" at "java:org.exist.jms.xquery.JmsModule";

declare variable $m:jmsConfiguration := map {
"java.naming.factory.initial" : "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
"java.naming.provider.url" : "tcp://localhost:61616",
"destination" : "dynamicQueues/eXistdb-messaging-example_countTest",
"connection-factory" : "ConnectionFactory"
};

declare variable $m:messageProperties := map {
"Su" : "Sunday",
"Mo" : xs:integer(1)
};

declare function local:handleMessage($content as item(), $params as item()*, $messageProperties as map(), $jmsConfig as map() )
{
    util:log("INFO", $content)
};


declare
%test:assertEquals(0,1,2,2,0)
function m:register()  {

    (: Define HoF callback function, with 4 parameters :)
    let $callback := local:handleMessage#4

    (: reset :)
    let $init := for $i in jms:list() return jms:close($i)

    (: should be 0 :)
    let $count0 := count(jms:list())

    (: register listener :)
    let $reg1 := messaging:register( $callback ,(), $m:jmsConfiguration)

    (: should be 1 :)
    let $count1 := count(jms:list())

    (: register listener :)
    let $reg2 := messaging:register( $callback ,(), $m:jmsConfiguration)

    (: should be 2 :)
    let $count2 := count(jms:list())

    (: stop listeners :)
    let $reset := for $i in jms:list() return jms:stop($i)

    (: should be 2 :)
    let $count3 := count(jms:list())

    (: close listeners :)
    let $reset := for $i in jms:list() return jms:close($i)

    (: should be 0 :)
    let $count4 := count(jms:list())

    return ($count0, $count1, $count2, $count3, $count4)
};