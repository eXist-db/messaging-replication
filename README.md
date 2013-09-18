messaging-replication
=====================

eXist-db extension for JMS based Messaging and document replication.

Please note that this repository is a copy from the original code of eXist-db/develop
which shall be removed soon.

This is work in progress. The build script is not perfect yet.


### How to build


First edit the build.properties and configure the EXIST_HOME location. The directory 
must contain a compiled instance of eXist-db.

```
  # clone from github
  git clone
  
  # download ivy dependency manager
  ant setup
  
  # build
  ant
```


### Available build targets

```
ant -projecthelp
Buildfile: /....../messaging-replication/build.xml
Build descriptor for the messaging-replication extension of eXist-db
Main targets:

 clean      Clean up all generated files
 clean-all  Reset to clean state
 compile    Compile java sources
 download   Download 3d party JAR files
 install    Install jar files into ${exist.dir}
 jar        Create JAR file
 prepare    Create empty directories
 setup      Download the Ivy dependency manager
 xar        Create XAR files
Default target: xar
```

  

