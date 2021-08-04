## Aurora And Database Migration Labs

This github repo contains Aurora MySQL and PostgreSQL Labs, Aurora Serverless Lab and Heterogeneous database migration with DMS Labs.

## Current Labs

[Aurora MySQL](https://github.com/aws-samples/aurora-and-database-migration-labs/blob/master/Labs/Aurora%20MySQL/Aurora%20MySQL%20Hands%20On%20Lab%20Manual%202.1.pdf) - In this lab you will create Aurora MySQL DB Cluster, modify security group to allow access to the Aurora MySQL DB instance from your computer, load data from S3 into Aurora MySQL database, create read replica instance & access table, create a database copy using Aurora "Clone" feature, and perform DML query on primary DB and validate data on primary and cloned DB copy.

[Aurora MySQL Serverless](https://github.com/aws-samples/aurora-and-database-migration-labs/blob/master/Labs/Aurora%20MySQL/Amazon%20Aurora%20Serverless%20for%20MySQL%20Lab.pdf) - In this lab you will create and configure a new Aurora Serverless DB cluster, create a Cloud9 client environment and then enable network traffic to the Aurora Serverless cluster from your Cloud9 environment, setup sysbench on the Cloud9 environment and run load test using the provided script, clean up and terminate your Cloud9 environment and Aurora Serverless DB cluster.

[Aurora MySQL Serverless Data API Live Demo](https://github.com/aws-samples/aurora-and-database-migration-labs/blob/master/Labs/Aurora%20MySQL/Aurora%20MySQL%20Serverless%20Data%20API%20Live%20Demo.pdf) - In this demo you will setup an Amazon Pinpoint project, configure a long code with 2 way SMS enabled; setup networking infrastructure, Aurora MySQL Serverless cluster and bootstrap the database using AWS Lambda; enable Data API for Aurora serverless cluster. You will use AWS Cloudformation to setup most of the components. Finally you will use the provisioned long code to send your vote and receive survery confirmation.

[Database Modernization Hands On Workshop](https://github.com/aws-samples/aurora-and-database-migration-labs/tree/master/Labs/amazon-rds-purpose-built-workshop) - A tutorial for developers, DBAs and data engineers to get hands-on experience on how to migrate relational data to AWS purpose-built databases such as Amazon DynamoDB and Amazon Aurora using AWS DMS and build data processing applications on top of it. You’ll get hands-on experience to evaluate the application use cases, decide on the target data store, and migrate a sample relational schema into AWS cloud using AWS DMS. You’ll leverage AWS services such as Amazon DynamoDB, Amazon S3, and Amazon relational database service (RDS).

[PostgreSQL Snapper](https://github.com/aws-samples/aurora-and-database-migration-labs/blob/master/Labs/PostgreSQL%20Snapper/PostgreSQL%20Snapper%20Lab.pdf) - The [PostgreSQL Snapper tool](https://github.com/aws-samples/aurora-and-database-migration-labs/tree/master/Code/PGPerfStatsSnapper) enables periodic collection (snapping) of PostgreSQL performance related statistics and metrics. The config file used by the tool can be customized to add and remove database dictionary views and queries to be snapped as required. Snapper collects and stores the PostgreSQL database metrics in separate OS level files to have minimal impact on the database. These files can be loaded into another PostgreSQL instance by the loader script for doing analysis. In this lab, you will configure Snapper to collect PostgreSQL statistics every 1 minute, generate load on Aurora PostgreSQL using pgbench, package and import Snapper Output to a PostgreSQL database for analysis and then derive insights from the PostgreSQL statistics data using sample SQLs included in the tool.

## License Summary

This sample code is made available under a modified MIT license. See the LICENSE file.
