xquery version "3.0";

import module namespace jms="http://exist-db.org/xquery/messaging" at "java:org.exist.messaging.xquery.MessagingModule";

(: 
 : The (callback) function is required to have 4 parameters:
 : 
 : $content The actual data of the messages
 : $params  Additional parameters set when registerering the callback function
 : $messageProperties JMS message properties, contains data of sender and the eXist-db database
 : $jmsConfig Detals about the JMS configuration (for debugging) 
 :)
declare function local:handleMessage($content as item(), $params as item()*, $messageProperties as map(), $jmsConfig as map() )
{
    ( 
        util:log-system-out( data($content) ), 
        util:log-system-out( $params ), 
        util:log-system-out( map:keys($messageProperties) ), 
        util:log-system-out( map:keys($jmsConfig) )
    )
};

(: Configuration for setting-up an JMS connection :)
let $jmsConfiguration :=
    map {
        "java.naming.factory.initial" := "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        "java.naming.provider.url" := "tcp://myserver.local:61616",
        "destination" := "dynamicQueues/eXistdbTest",
        "connection-factory" := "ConnectionFactory"
    }
    
(: Define HoF callback function, with 4 parameters :)
let $callback := local:handleMessage#4 

(: Additional (optional) data to parameterize the callback function :)
let $additionalParameters := (1,2,3)
    
return
    (: Register the function to the JMS broker :)
    jms:register( $callback , $additionalParameters, $jmsConfiguration)