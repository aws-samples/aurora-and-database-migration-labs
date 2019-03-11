## Amazon RDS Data API Demo

Its an example Lambda function which showcases how to run queries using SDK for Aurora Serverless Data API.

The Lambda app runs a SQL INSERT and SELECT using the Data API.

https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html


## Requirements

This package requires gradle for building it.

    gradle build

This will generate a zip file under build/distributions that can be uploaded to AWS Lambda for running it.


## License Summary

This sample code is made available under a modified MIT license. See the LICENSE file.
