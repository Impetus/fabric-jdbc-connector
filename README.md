 
 
Overview 
========= 
[![Join the chat at https://gitter.im/Impetus/fabric-jdbc-connector](https://badges.gitter.im/Impetus/fabric-jdbc-connector.svg)](https://gitter.im/Impetus/fabric-jdbc-connector?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) 
**[Join Google Group ](https://groups.google.com/forum/#!forum/fabric-jdbc-driver)**
 
Fabric JDBC connector implements a pure java, type 4 JDBC driver that executes SQL queries on Hyperledger fabric blockchain. It facilitates getting the data in and out of fabric in JDBC compliant manner. The Fabric JDBC Connector can be used to perform ETL, BI reporting and analytics using the familiar SQL language.  
 
It uses [blkchn-sql-driver](https://github.com/Impetus/blkchn-sql-driver) to parse the query and create corresponding logical plan. This logical plan is then converted into an optimized physical plan. The driver extends and implements the physical plan, using corresponding function calls of [fabric-sdk-java](https://github.com/hyperledger/fabric-sdk-java) to interact with Fabric network. The driver then converts the returned objects to a JDBC compliant result set and return it to the user. 
 
 

# Tested with
| Dependency | Version |
|---|---|
| maven | 3.3.3 |
|java | 1.8 |
|fabric-sdk-java | 1.1.0 |
|spark|2.0.2|
 
Features Added with Release 1.1.0
=====
* Created Spark Connector Module: Use spark connector to connect Spark with fabric.
* Create User : allows creation of new user with certificate authority
* Install Only : This option can be given with Create and upgrade chaincode syntax.
* Instantiate Only : This option can be used with create chaincode syntax.
* Upgrade Only: It can be used with upgrade chaincode syntax.   
* Enrolment Certificate is stored in-memory. 

Supported Features  
=============== 

- Querying on blocks, transactions, transaction actions and read write sets
- Creating Assets
- Querying Assets 
- Deleting Assets
- Creating Asset schema
- Dropping Asset schema  
- Creating Chaincode
- Upgrading Chaincode
- Create User

Building fabric-jdbc-connector
==========================
- [Download](https://github.com/Impetus/blkchn-sql-driver/archive/master.zip) or clone <b>blkchn-sql-driver</b> project `git clone https://github.com/Impetus/blkchn-sql-driver.git`
- build blkchn-sql-driver using `mvn clean install -Pgen-sources`
- [Download](https://github.com/Impetus/fabric-jdbc-connector/archive/master.zip) <b>fabric-jdbc-connector</b> source code or use `git clone https://github.com/Impetus/fabric-jdbc-connector.git` 
- build it using `mvn clean install` 

Getting Started 
===============  


- Navigate to [examples](https://github.com/Impetus/fabric-jdbc-connector/tree/master/fabric-jdbc-examples) folder 
- Run [`App.java`](https://github.com/Impetus/fabric-jdbc-connector/blob/master/fabric-jdbc-examples/src/main/java/com/impetus/fabric/example/App.java) for quick start 
 
 
Fabric JDBC connector is a maven based project. It can be directly added as maven dependency in your project in the following manner : 
  
  
* Add below in dependency: 
  
``` 
<dependency> 
<groupId>com.impetus.fabric</groupId> 
<artifactId>fabric-jdbc-driver</artifactId> 
<version>${fabricjdbcdriver.version}</version> 
</dependency> 
``` 
 
Build your project with the above changes to your pom.xml. 
 

Recovering Disksapce from Docker 
=================================
If you have run example code or integration test. It will create docker images, that need to clean manually. it consume
 approximately 100 MB of space upon running integration test once.

You need to run these command to free up space.
- remove exited docker process.  
```
    docker rm -v $(docker ps -a -q -f status=exited)
```
- remove images by giving chaincode name pattern as in below command   
```
    docker rmi -f `docker images|grep -i chncodefunc|awk '{ print $3 }'`
    docker rmi -f `docker images|grep -i assettransfer|awk '{ print $3 }'`
    docker rmi -f `docker images|grep -i sacc-1.0|awk '{ print $3 }'`
```
- Delete dangling volume occupied, if everything is clean, it will give message: "requires a minimum of 1 argument"  
```
    docker volume rm $(docker volume ls -qf dangling=true) 
```
 
Important Links 
=============== 
* [Fabric JDBC Connector Wikis](https://github.com/Impetus/fabric-jdbc-connector/wiki) 
 
  
 
Contribution 
============ 
* [Contribution Guidelines](https://github.com/Impetus/fabric-jdbc-connector/blob/master/CONTRIBUTING.md) 
 
About Us 
======== 
Fabric JDBC Connector is backed by Impetus Labs - iLabs. iLabs is a R&D consulting division of [Impetus Technologies](http://www.impetus.com). iLabs focuses on innovations with next generation technologies and creates practice areas and new products around them. iLabs is actively involved working on blockchain technologies, neural networking, distributed/parallel computing and advanced analytics using spark and big data ecosystem. iLabs is also working on various other Open Source initiatives. 
 
 
