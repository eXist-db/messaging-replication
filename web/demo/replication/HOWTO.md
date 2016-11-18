# Howto.... test replication

## Prerequisites

To test replication of document changes between two eXist-db instances the following
setup is required:
- One ActiveMQ broker
- Two eXist-db databases with the messaging-replication extension installed.

## Start document 'receiver'
On the 'receiving' eXist-db instance 
- Open eXide as 'admin'
- Edit the file `startListener.xq`
- Update the value of `java.naming.provider.url` to match the address of the broker.
- Start the query by clicking the `Eval` button

## Configure document 'sender'
On the 'sender' eXist-db instance 
- Open eXide as 'admin'
- Edit the file `collection.xconf`
- Update the value of `java.naming.provider.url` to match the address of the broker.
- Save the document by clicking the `Save` button
- Click "OK" for the question "You have saved a collection configuration file. Would you like to
   apply it to collection /db/apps/messaging-replication/demo/replication now?" 
- The `collection.xconf` document is now installed as
   `/db/system/config/db/apps/messaging-replication/demo/replication/collection.xconf`
   
## Start sending document updates
On the 'sending' eXist-db instance 
- Open eXide as 'admin' 
- Open the query `createDocuments.xq` and click the `Eval` button
- Documents are created in the `replicated` collection in the 'sender'
- Documents are sent to and created on the 'reciever' in the same collection.
- Repeat the same steps wih the `removeDocuments.xq` to delete documents on 
  both 'sender' and 'receiver' side
  
# Diagnose
- eXist-db logging: `$EXIST_HOME/webapp/WEB-INF/logs/exist.log`
- ActiveMQ logging: `$ACTIVEMQ_HOME/data/activemq.log`
- ActiveMQ console: `http://<servername>:8161/admin/`