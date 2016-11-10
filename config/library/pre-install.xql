xquery version "3.0";

if(sm:find-groups-by-groupname("jms")="jms") then () else sm:create-group("jms", "JMS messaging/replication extension")