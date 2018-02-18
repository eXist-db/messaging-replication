# eXist-db messaging-replication extension

This is the eXist-db extension providing 'messaging' and 'document replication' features based on [JMS](http://en.wikipedia.org/wiki/Java_Message_Service) technology.

# Documentation

Please consult the [WiKi](https://github.com/eXist-db/messaging-replication/wiki) for documentation and more information about this extension.

The git repository and `XAR` application contain a number or ready-to-go examples for the [messaging](https://github.com/eXist-db/messaging-replication/tree/develop/web/demo/messaging) and the [replication](https://github.com/eXist-db/messaging-replication/tree/develop/web/demo/replication) features. 

> It is highly recommended is to start with the messaging feature first before configuring the more complex replication feature. Following this order will make sure that the infrastructure is correctly setup.


# Sponsors

The extension has been made possible with the funds of a number of [sponsors](https://github.com/eXist-db/messaging-replication/wiki/Sponsors). 

# How to build

The extension is built with maven (version 3.5.2 is required for the used `kuberam-expath-plugin`)

```
mvn clean package
```
