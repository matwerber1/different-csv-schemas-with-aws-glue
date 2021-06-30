import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as glue from '@aws-cdk/aws-glue';
import * as iam from '@aws-cdk/aws-iam';
import * as s3deploy from '@aws-cdk/aws-s3-deployment';


export class CdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const account = cdk.Stack.of(this).account;

    const bucketName = `${account}-datalake`;
    const S3TransactionPathCSV = `s3://${bucketName}/raw/csv/transactions/`
    const S3TransactionPathParquet = `s3://${bucketName}/raw/parquet/transactions/`

    const bucket = new s3.Bucket(this, 'bucket', {
      bucketName: bucketName,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    const database = new glue.Database(this, 'database', {
      databaseName: 'datalake'
    });

    const crawlerRole = new iam.Role(this, 'crawlerRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AWSGlueConsoleFullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('CloudWatchLogsFullAccess'),
      ]
    });

    const transactionCrawlerCSV = new glue.CfnCrawler(this, 'transactionCrawlerCSV', {
      role: crawlerRole.roleArn,
      databaseName: database.databaseName,
      name: "transaction_csv_crawler",
      targets: {
        s3Targets: [
          {
            path: S3TransactionPathCSV
          }
        ]
      },
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',   // LOG or UPDATE_IN_DATABASE
        deleteBehavior: 'DEPRECATE_IN_DATABASE'    // LOG, UPDATE_IN_DATABASE, or DEPRECATE_IN_DATABASE
      },
    });

    const transactionCrawlerParquet = new glue.CfnCrawler(this, 'transactionCrawlerParquet', {
      role: crawlerRole.roleArn,
      databaseName: database.databaseName,
      name: "transaction_parquet_crawler",
      targets: {
        s3Targets: [
          {
            path: S3TransactionPathParquet
          }
        ]
      },
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',   // LOG or UPDATE_IN_DATABASE
        deleteBehavior: 'DEPRECATE_IN_DATABASE'    // LOG, UPDATE_IN_DATABASE, or DEPRECATE_IN_DATABASE
      },
    });

    const s3FileDeployment = new s3deploy.BucketDeployment(this, 'DeployTestFiles', {
      sources: [
        s3deploy.Source.asset('./lib/data')
      ],
      destinationBucket: bucket,
    });

  }
}
