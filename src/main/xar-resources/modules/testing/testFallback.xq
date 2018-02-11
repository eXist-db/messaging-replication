xquery version "3.0";

import module namespace messaging="http://exist-db.org/xquery/messaging" 
              at "java:org.exist.jms.xquery.MessagingModule";
                        
    
declare function local:handleMessageOK($content as item(), $params as item()*, 
                            $messageProperties as map(), $jmsConfig as map() )
{
    (util:log("info", data($content)), "log")
};       

declare function local:handleMessageNOK($content as item(), $params as item()*, 
                            $messageProperties as map(), $jmsConfig as map() )
{
    fn:error( fn:QName('http://www.w3.org/2005/xqt-errors', 'err:FOER0000') ) 
};

let $jmsConfiguration :=
    map {
        "java.naming.factory.initial" 
            := "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        "java.naming.provider.url" := "tcp://localhost:61616",
        "destination" := "dynamicQueues/eXistdb-test",
        "connection-factory" := "ConnectionFactory"
    }

(: Define HoF callback function, with 4 parameters :)
let $ok1 := local:handleMessageOK#4  
let $ok2 := local:handleMessageOK#4 
let $nok := local:handleMessageNOK#4 
let $ok3 := local:handleMessageOK#4 
let $ok4 := local:handleMessageOK#4 

let $additionalParameters := (1, "2" , xs:float(3.0))

let $register := (messaging:register( $ok1 , $additionalParameters, $jmsConfiguration),
    messaging:register( $ok2 , $additionalParameters, $jmsConfiguration),
    messaging:register( $nok , $additionalParameters, $jmsConfiguration),
    messaging:register( $ok3 , $additionalParameters, $jmsConfiguration),
    messaging:register( $ok4 , $additionalParameters, $jmsConfiguration))
    
return $register