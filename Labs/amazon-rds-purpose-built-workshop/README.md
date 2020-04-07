# Database Modernization Hands On Workshop

A tutorial for developers, DBAs and data engineers to get hands-on experience on how to migrate relational data to AWS purpose-built databases such as Amazon DynamoDB and Amazon Aurora using AWS DMS and build data processing applications on top of it.



# Overview: Working with AWS Purpose-Built databases

The days of one-size-fits-all, monolithic databases are behind us. Database design and management requires a different mindset in AWS compared to traditional relational database management system (RDBMS) design. In this workshop, we will demonstrate how to leverage both relational (Amazon Aurora) and non-relational databases (Amazon DynamoDB) that are purpose-built to handle the specific needs of an application. For illustrative purpose, we will leverage a relational schema used by a taxi application.

## Pre-requisites

If you are doing this lab as part of a workshop conducted by AWS, all the CloudFormation templates required by this lab will be pre-installed in the provided AWS account. 

If you are doing this lab on your own, you need to have an AWS account with IAM administrator privileges. You can launch the [lab CloudFormation template](./src/cloudformation.template) in [us-east-1](https://console.aws.amazon.com/console/home?region=us-east-1) AWS region to setup the required resources for this lab.

We will leverage the following AWS services as part of this workshop.

- [VPC](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Scenario2.html) with Public and Private subnets, NAT Gateway and Route tables 
- [Oracle RDS](https://aws.amazon.com/rds/oracle/) instance launched from a snapshot  preloaded with a sample taxi schema . This will be used as a source for our migration.
- [Amazon Aurora PostgreSQL](https://aws.amazon.com/rds/aurora/postgresql-features/) as a target for relational data
- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) as a target for NoSQL data
- [AWS DMS](https://aws.amazon.com/dms/) for migrating database from source to target
- [AWS Lambda](https://aws.amazon.com/lambda/) for event driven data processing
- [AWS Cloud9](https://aws.amazon.com/cloud9) IDE  for running scripts and deploying code
- [IAM Roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) for permissions required by AWS DMS

:warning: **You will be billed for the AWS resource usage if you running this lab on your own AWS Account. Make sure to delete the CloudFormation stacks and AWS resources created by this Lab after you are done to avoid incurring additional charges.** 
 
## (Optional) SQL Client Installation
  Download and install a SQL Client in your laptop which can connect to both Oracle and PostgreSQL if you don't already have one. Suggested open source tools which can work with both Oracle and PostgreSQL are provided below. Please note that these tools require [PostgreSQL JDBC](https://jdbc.postgresql.org/) and [Oracle JDBC](https://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html) drivers for connectivity. 


 - [dbeaver Community Edition](https://dbeaver.io/download/)
         
   
 - [SQL-Workbench](https://www.sql-workbench.eu/downloads.html)

 
For step-by-step instructions on how to configure SQL-Workbench to connect to Oracle/PostgreSQL instances, please refer to AWS Documentation ([PostgreSQL](https://aws.amazon.com/getting-started/tutorials/create-connect-postgresql-db/) and [Oracle](https://docs.aws.amazon.com/dms/latest/sbs/CHAP_RDSOracle2Aurora.Steps.ConnectOracle.html)).

  > **_NOTE:_** For doing these Labs, you don't need an Oracle client or any GUI based client. An Oracle client is required only if you want to explore the sample data in the source Oracle database.  For working with Aurora PostgreSQL database, you can leverage [psql](https://www.postgresql.org/docs/9.5/app-psql.html) command line utility. We will install this as part of Lab 1.


## Workshop Details

**Lab 1**: In this lab, you will be performing a migration of sample taxi data from RDS Oracle to Amazon DynamoDB and Amazon Aurora PostgreSQL databases using AWS DMS.

**Lab 2**: In this lab, you will simulate taxi trip booking by a rider and acceptance by a driver followed by billing and payment using Python scripts and SQL commands. You will utilize DynamoDB streams and AWS lambda functions to insert completed trip data from DynamoDB to Aurora PostgreSQL.

**Lab 3**: In this lab, we will leverage Athena federated query feature (**_in preview_**) to query both DynamoDB and Aurora trip data using a single SQL query. You will also utilize this feature to query DynamoDB and data stored in Amazon S3.


|Lab|Name|
|---|----|
|Lab 1|[Taxi Data Migration using AWS DMS](./lab1-TaxiDataMigration)|
|Lab 2|[Taxi Booking, Billing and Payments](./lab2-TaxiBookingAndPayments)|
|Lab 3|[Query multiple data stores using Athena Federated Query](./lab3-AthenaFederatedQuery)|


## License Summary

The documentation is made available under the Creative Commons Attribution-ShareAlike 4.0 International License. See the LICENSE file.

The sample code within this documentation is made available under the MIT-0 license. See the LICENSE-SAMPLECODE file.
