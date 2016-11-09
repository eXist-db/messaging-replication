xquery version "3.1";

(:~
: Simple script to remove resources to test the replication of deletion the resources.
:)

import module namespace config = "http://exist-db.org/extension/jms/config" at "../../modules/config.xqm";

let $root := $config:app-root || "/demo/replication/replicated"

for $document in xmldb:get-child-resources($root)
return
    xmldb:remove($root, $document)
