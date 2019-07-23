xquery version "3.0";

(:~
: Example to receive messages and write the payload plus properties into a document
:)


import module namespace messaging="http://exist-db.org/xquery/messaging" at "java:org.exist.jms.xquery.MessagingModule";
import module namespace config = "http://exist-db.org/extension/jms/config" at "../../../modules/config.xqm";

(:
 : A (callback) function is required to have 4 parameters. This function
 : logs incoming data to "standard out".
 :
 : $content The actual data of the messages
 : $params  Additional parameters set when registerering the callback function
 : $messageProperties JMS message properties, contains data of sender and
 :                    the eXist-db database
 : $jmsConfig Detals about the JMS configuration (for debugging)
 :)
declare function local:handleMessage($content as item(), $params as item()*,
                            $messageProperties as map(), $jmsConfig as map() )
{
    util:log-system-out( "eval result:" || util:eval("count(collection(/db))") )
};



let $destination := $config:app-root || "/demo/messaging/queues/messages"

(: Configuration for setting-up an JMS connection :)
let $jmsConfiguration := map {
    "java.naming.factory.initial": "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    "java.naming.provider.url":    "tcp://localhost:61616",
    "destination":                 "dynamicQueues/eXistdb-messaging-example",
    "connection-factory":          "ConnectionFactory"
}

(: Define HoF callback function, with 4 parameters :)
let $callback := local:handleMessage#4

(: Additional (optional) data to parameterize the callback function :)
let $additionalParameters := ( $destination )

return
    (: Register the function to the JMS broker :)
    messaging:register( $callback , $additionalParameters, $jmsConfiguration)

