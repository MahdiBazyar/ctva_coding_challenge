Code Challenge Template

Environment Setup
Python Version: 3.8
Database: postgresql version 16.3 
Source-code editor: Visual Studio Code 


Input data source in the below repository:

https://github.com/corteva/code-challenge-template

Weather Data Description

------------------------

The wx_data directory has files containing weather data records from 1985-01-01 to 2014-12-31. Each file corresponds to a particular weather station from Nebraska, Iowa, Illinois, Indiana, or Ohio.


Each line in the file contains 4 records separated by tabs: 


1. The date (YYYYMMDD format)

2. The maximum temperature for that day (in tenths of a degree Celsius)

3. The minimum temperature for that day (in tenths of a degree Celsius)

4. The amount of precipitation for that day (in tenths of a millimeter)


Missing values are indicated by the value -9999.

Problem 1 - Data Modeling

-------------------------

Choose a database to use for this coding exercise. Design a data model to represent the weather data records. 
For this project, Postgres has been utilized to store and manage data. All queries to create tables and perform analysis are in postgres folder and in below sql files:
weather_table.sql
weather_analysis.sql 

Problem 2 - Ingestion

---------------------

Write code to ingest the weather data from the raw text files supplied into your database, using the model you designed. Check for duplicates: if your code is run twice, you should not end up with multiple rows with the same data in your database. Your code should also produce log output indicating start and end times and number of records ingested.
For this project, the main Python file is weather_data.py where main logic to read text data, clean the data, ingest data into postgres database, and perform weather data analysis is implemented.
The main python file reads configurable parameters from a configuration file name weather_data_config.json. When you are going to run the code, you can modify configuraiton file per you specific environment requirements.
Some base functions to create log files and load configuation file are implemented in base_functions.py that are imported by main python file in weather_data.py. 
The code handle duplicate records and provides inputs on total records inserted in database along with process run time in the log flie. 

Problem 3 - Data Analysis

-------------------------

For every year, for every weather station, calculate:

* Average maximum temperature (in degrees Celsius)

* Average minimum temperature (in degrees Celsius)

* Total accumulated precipitation (in centimeters)

Ignore missing data when calculating these statistics.

Design a new data model to store the results. Use NULL for statistics that cannot be calculated.


For this project, my answer includes the new model definition as well as the code used to calculate the new values and store them in the database.

I've included some screenshots respresenting the result of my data analysis in the answers folder. 



Problem 4 - REST API

--------------------

Choose a web framework (e.g. Flask, Django REST Framework). Create a REST API with the following GET endpoints:

/api/weather

/api/weather/stats

Both endpoints should return a JSON-formatted response with a representation of the ingested/calculated data in your database. Allow clients to filter the response by date and station ID (where present) using the query string. Data should be paginated.

Include a Swagger/OpenAPI endpoint that provides automatic documentation of your API.


Your answer should include all files necessary to run your API locally, along with any unit tests.


Extra Credit - Deployment

-------------------------

**Assume you are asked to get your code running in the cloud using AWS. What tools and AWS services would you use to deploy the API, database, and a scheduled version of your data ingestion code? Write up a description of your approach.

To deploy the API, database, and scheduled data ingestion code in the cloud using AWS, we can leverage several AWS services and tools. Below is a detailed approach to achieving this deployment:

1. Database Setup: Amazon RDS (Relational Database Service)
Service: Amazon RDS for PostgreSQL

Description: Amazon RDS makes it easy to set up, operate, and scale a relational database in the cloud. It provides cost-efficient and resizable capacity while automating time-consuming administration tasks.
Steps:
Create an RDS instance:
Use the AWS Management Console to launch an RDS instance with PostgreSQL.
Configure the instance (instance type, storage, VPC, security groups, etc.).
Set up the database credentials and parameters.
Configure security:
Ensure the RDS instance is in a private subnet within a VPC for security.
Set up security groups to allow access only from trusted IP ranges and services.
2. API Deployment: AWS Lambda and API Gateway
Services: AWS Lambda, Amazon API Gateway

Description: AWS Lambda allows you to run code without provisioning or managing servers. Amazon API Gateway enables you to create, publish, maintain, monitor, and secure APIs at any scale.
Steps:
Create a Lambda function:
Write a Python Lambda function to handle API requests and interact with the RDS PostgreSQL database.
Package the code, including any dependencies, and upload it to AWS Lambda.
Set up API Gateway:
Create a new API using API Gateway.
Define the endpoints and methods (e.g., GET, POST) that will trigger the Lambda function.
Configure API Gateway to invoke the Lambda function when requests are received.
Security:
Use IAM roles and policies to grant the Lambda function the necessary permissions to access the RDS database.
Enable API Gateway security features like API keys, usage plans, and AWS WAF for additional security.
3. Scheduled Data Ingestion: AWS Lambda and Amazon CloudWatch Events
Services: AWS Lambda, Amazon CloudWatch Events (EventBridge)

Description: AWS Lambda can be triggered on a scheduled basis using CloudWatch Events. This setup allows for automated, scheduled data ingestion processes.
Steps:
Create a data ingestion Lambda function:
Write a Lambda function to read data from text files, process it, and insert it into the PostgreSQL database.
Package and deploy this Lambda function to AWS Lambda.
Set up CloudWatch Events:
Create a CloudWatch Events rule to trigger the Lambda function on a schedule (e.g., daily, hourly).
Define the schedule using a cron expression or rate expression.
Configure the rule to invoke the data ingestion Lambda function.
4. Infrastructure as Code: AWS CloudFormation or AWS CDK
Tools: AWS CloudFormation, AWS CDK (Cloud Development Kit)

Description: Use Infrastructure as Code (IaC) to automate the deployment and management of AWS resources. AWS CDK provides a high-level, reusable way to define cloud resources in code.
Steps:
Define resources:
Use AWS CDK or CloudFormation templates to define the RDS instance, Lambda functions, API Gateway, and CloudWatch Events rules.
Deploy the stack:
Use the AWS CDK CLI or AWS Management Console to deploy the CloudFormation stack.
Manage updates and versioning of the infrastructure through code.
5. Monitoring and Logging: Amazon CloudWatch and AWS X-Ray
Services: Amazon CloudWatch, AWS X-Ray

Description: Monitor and troubleshoot the deployed services using CloudWatch for logs and metrics, and X-Ray for tracing.
Steps:
Enable logging:
Configure CloudWatch Logs for Lambda functions to capture logs.
Set up CloudWatch Alarms to notify on any critical issues or thresholds.
Enable tracing:
Use AWS X-Ray to trace requests through the API Gateway and Lambda functions for detailed performance insights.
Summary Diagram of AWS Services
Amazon RDS: Hosts the PostgreSQL database.
AWS Lambda: Runs the API and scheduled data ingestion code.
Amazon API Gateway: Provides endpoints for the API.
Amazon CloudWatch Events: Schedules the Lambda functions for data ingestion.
Amazon CloudWatch: Monitors logs and performance.
AWS X-Ray: Provides request tracing for debugging and performance analysis.
By using these AWS services and tools, we can deploy a robust, scalable, and secure solution for data ingestion and analysis in the cloud. This approach leverages the strengths of serverless computing for the API and scheduled tasks, combined with managed database services for reliable data storage.
