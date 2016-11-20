# Howto.... test messaging

## Prerequisites

To test messaging o between two eXist-db instances the following
setup is required:
- One ActiveMQ broker
- One (!) eXist-db database with the messaging-replication extension installed.

> There are two collections, one to demonstrate "queues" and one to demonstrate 
"topics". The documents are almost identical except the `destination` value, and a few additional
requiper parameters for durable topics.

## Start message 'receiver'
On the 'receiving' eXist-db instance 
- Open eXide as 'admin'
- Edit the file `registerReceiver.xq`
- Update the value of `java.naming.provider.url` to match the address of the broker.
- Save the query
- Start the query by clicking the `Eval` button

## Configure 'sender'
On the 'receiving' eXist-db instance 
- Open eXide as 'admin'
- Edit the file `sendMessage.xq`
- Update the value of `java.naming.provider.url` to match the address of the broker.
- Save the query
   
## Send messages
On the 'sending' eXist-db instance 
- Open eXide as 'admin' 
- Open the query `sendMessage.xq` and click the `Eval` button
- Documents are created in the `messages` collection, containing the payload and 
  the message poperties of the JMS message.
- Clean the documents using the `removeMessages.xq` query
  
# Diagnose
- eXist-db logging: `$EXIST_HOME/webapp/WEB-INF/logs/exist.log`
- ActiveMQ logging: `$ACTIVEMQ_HOME/data/activemq.log`
- ActiveMQ console: `http://<servername>:8161/admin/`
