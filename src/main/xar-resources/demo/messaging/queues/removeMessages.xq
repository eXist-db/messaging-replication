xquery version "3.0";

(:~
: Simple script to remove documents received by the messaging-listener
:)

import module namespace config = "http://exist-db.org/extension/jms/config" at "../../../modules/config.xqm";

let $root := $config:app-root || "/demo/messaging/queues/messages"

for $document in xmldb:get-child-resources($root)
return
    xmldb:remove($root, $document)
