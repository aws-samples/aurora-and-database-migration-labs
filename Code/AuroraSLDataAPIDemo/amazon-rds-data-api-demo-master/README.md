## Amazon RDS Data API Demo

Its an example Lambda function which showcases how to run queries using SDK for Aurora Serverless Data API.

The Lambda app runs a SQL INSERT and SELECT using the Data API.

https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html


## Requirements

This package requires gradle for building it.

    gradle build

This will generate a zip file under build/distributions that can be uploaded to AWS Lambda for running it.

## Configuration
Running this Lambda function requires the following setup:

1) Setup Amazon Pinpoint Application and provision and Long Code for sending/receiving SMS.
2) Aurora Mysql Serverless database with Data API enabled.
3) A Secret store which contains username/password for the database.
4) Further the demo depends on the following table in the database to be created:

    CREATE DATABASE Demo;
    
    CREATE TABLE Demo.Cities(City varchar(255));

## How to use the Demo Lambda Application

1) Create a new Lambda function with name CityNameHandler, and Handler "CityVoteHandler::handleRequest"

2) Modify CityVoteHandler.java to include your database ARN and Secret store ARN.

3) Modify SMSSender.java to include your Amazon Pinpoint Application ID and Long Code

4) Run 'gradle build' and upload the zip file created to the lambda function

5) Setup AmazonPinpoint to invoke Lambda which it receives a SMS. Example Setup:
    * https://aws.amazon.com/blogs/aws/aws-pinpoint-launches-two-way-text-messaging/

6) Modify the Lambda IAM role to add following permissions:

Permissions to call ExecuteSql API for the Aurora Serverless database.

    {
        "Effect": "Allow",
        "Action": "rds-data:ExecuteSql",
        "Resource": "ARN of Aurora MySQL Serverless Database"
    }

Permissions to call AmazonPinpoint to send SMS

    {
        "Effect": "Allow",
        "Action": "mobiletargeting:SendMessages",
        "Resource": "arn:aws:mobiletargeting:region:account-id:apps/project-id/messages"
    }
Permissions to the secret store with the database username/password

    {
        "Effect": "Allow",
        "Action": "secretsmanager:GetSecretValue",
        "Resource": "ARN of Secret which contains the username/password"
    }


## Running Demo Lambda

Just send a SMS with city name, e.g. Seattle, to the Long Code that you provisioned, and you should get a reply back.
Also the city name will be stored in your Aurora database by the Lambda function using Data API.
The number of people from same city will be counted using Select statement, using Data API as well.

## License Summary

This sample code is made available under a modified MIT license. See the LICENSE file.
