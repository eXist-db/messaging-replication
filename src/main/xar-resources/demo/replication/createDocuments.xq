xquery version "3.0";

(:~
: Simple script to create a small number of resources to test replication of creation of the resources.
:)

import module namespace config = "http://exist-db.org/extension/jms/config" at "../../modules/config.xqm";

let $root := $config:app-root || "/demo/replication/replicated"

for $i in (1000 to 1010)
return
    xmldb:store($root, "mydoc" || $i || ".xml", <doc>{ util:uuid() }</doc>)

