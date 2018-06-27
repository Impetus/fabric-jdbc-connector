 
 
Overview 
========= 
 
Fabric JDBC connector implements a pure java, type 4 JDBC driver that executes SQL queries on Hyperledger fabric blockchain. It facilitates getting the data in and out of fabric in JDBC compliant manner. The Fabric JDBC Connector can be used to perform ETL, BI reporting and analytics using the familiar SQL language.  
 
It uses [blkchn-sql-driver](https://github.com/Impetus/blkchn-sql-driver) to parse the query and create corresponding logical plan. This logical plan is then converted into an optimized physical plan. The driver extends and implements the physical plan, using corresponding function calls of [fabric-sdk-java](https://github.com/hyperledger/fabric-sdk-java) to interact with Fabric network. The driver then converts the returned objects to a JDBC compliant result set and return it to the user. 
 
 

# Tested with
| Dependency | Version |
|---|---|
| maven | 3.3.3 |
|java | 1.8 |
|fabric-sdk-java | 1.0.1 |
 

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

Building fabric-jdbc-connector
==========================
- [Download](https://github.com/Impetus/blkchn-sql-driver/archive/master.zip) or clone <b>blkchn-sql-driver</b> project `git clone https://github.com/Impetus/blkchn-sql-driver.git`
- build blkchn-sql-driver using `mvn clean install -Pgen-sources`
- [Download](https://github.com/Impetus/fabric-jdbc-connector/archive/master.zip) <b>fabric-jdbc-connector</b> source code or use `git clone https://github.com/Impetus/fabric-jdbc-connector.git` 
- build it using `mvn clean install` 

Getting Started 
===============  


- Navigate to [examples](https://github.com/Impetus/fabric-jdbc-connector/tree/master/fabric-sample) folder 
- Run [`App.java`](https://github.com/Impetus/fabric-jdbc-connector/blob/master/fabric-sample/src/main/java/com/impetus/fabricsample/App.java) for quick start 
 
 
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
 
 
Important Links 
=============== 
* [Fabric JDBC Connector Wikis](https://github.com/Impetus/fabric-jdbc-connector/wiki) 
 
  
 
Contribution 
============ 
* [Contribution Guidelines](https://github.com/Impetus/fabric-jdbc-connector/blob/master/CONTRIBUTING.md) 
 
About Us 
======== 
Fabric JDBC Connector is backed by Impetus Labs - iLabs. iLabs is a R&D consulting division of [Impetus Technologies](http://www.impetus.com). iLabs focuses on innovations with next generation technologies and creates practice areas and new products around them. iLabs is actively involved working on blockchain technologies, neural networking, distributed/parallel computing and advanced analytics using spark and big data ecosystem. iLabs is also working on various other Open Source initiatives. 
 
 