import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as events from 'aws-cdk-lib/aws-events';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as sfnTasks from 'aws-cdk-lib/aws-stepfunctions-tasks';

import { Construct } from 'constructs';

const EVENT_BUS_NAME = 'DzhOrchEventBus';

export class DzhOrchStack extends cdk.Stack {

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create resources
    const eventHistoryTable = this.createEventHistoryTable();
    const rdsEventStateTable = this.createRdsDatabase();
    const dlq = this.createDeadLetterQueue();

    // Create EventBus
    const eventBus = this.createEventBus();

    // Create Lambda functions
    const storeEventFunction = this.createStoreEventFunction(eventHistoryTable, dlq);

    const updateProductStateFunction = this.createUpdateProductStateFunction(rdsEventStateTable);
    const stateMachine = this.createStateMachine(updateProductStateFunction);

    const sendProductStateFunction = this.createSendProductStateFunction();

    const forwardToStateMachineFunction = this.createForwardToStateMachineFunction(stateMachine);

    eventBus.grantPutEventsTo(sendProductStateFunction);

    // Create API Gateway
    this.createApiGateway(sendProductStateFunction);

    // Grant permissions
    this.grantPermissions(eventHistoryTable, storeEventFunction, sendProductStateFunction);

    // Create EventBridge resources
    this.createEventBridgeRules(eventBus, storeEventFunction, updateProductStateFunction, forwardToStateMachineFunction, dlq);
  }

  private createEventHistoryTable(): dynamodb.Table {
    return new dynamodb.Table(this, 'EventHistoryTable', {
      partitionKey: { name: 'eventId', type: dynamodb.AttributeType.STRING },
      tableName: 'DzhOrch_EventHistory',
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });
  }

  private createDeadLetterQueue(): sqs.Queue {
    return new sqs.Queue(this, 'StoreEventDLQ', {
      queueName: 'StoreEventDLQ',
      retentionPeriod: cdk.Duration.days(14),
    });
  }

  private createStoreEventFunction(eventHistoryTable: dynamodb.Table, dlq: sqs.Queue): lambda.Function {
    return new lambda.Function(this, 'StoreEventFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'store_event.lambda_handler',
      code: lambda.Code.fromAsset('../lambda'),
      environment: {
        EVENT_HISTORY_TABLE: eventHistoryTable.tableName,
      },
      deadLetterQueue: dlq,
      deadLetterQueueEnabled: true,
    });
  }

  private createForwardToStateMachineFunction(stateMachine: sfn.StateMachine): lambda.Function {
    const forwardToStateMachineFunction = new lambda.Function(this, 'ForwardToStateMachineFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'forward_to_state_machine.lambda_handler',
      code: lambda.Code.fromAsset('../lambda'),
      environment: {
        EVENT_BUS_NAME: EVENT_BUS_NAME,
        STATE_MACHINE_ARN: stateMachine.stateMachineArn,
      },
    });

    stateMachine.grantStartExecution(forwardToStateMachineFunction);
    return forwardToStateMachineFunction;
  }

  // Create the Lambda function to update product state
  private createUpdateProductStateFunction(rdsDatabase: rds.DatabaseInstance): lambda.Function {
    const secret = rdsDatabase.secret; // Get the RDS secret from the database instance

    if (!secret) {
      throw new Error('RDS database secret is not available.');
    }

    const updateProductStateFunction = new lambda.Function(this, 'UpdateProductStateFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'update_product_state.lambda_handler',
      code: lambda.Code.fromAsset('../lambda'),
      environment: {
        DB_SECRET_ARN: secret.secretArn, // Pass the secret ARN
        DB_NAME: rdsDatabase.instanceIdentifier, // Pass the database name
      },
    });

    // Grant the Lambda function permission to read the secret
    secret.grantRead(updateProductStateFunction);

    return updateProductStateFunction;
  }

  private createSendProductStateFunction(): lambda.Function {
    return new lambda.Function(this, 'SendProductStateFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'send_product_state.lambda_handler',
      code: lambda.Code.fromAsset('../lambda'),
      environment: {
        EVENT_BUS_NAME: EVENT_BUS_NAME,
      },
    });
  }

  private createApiGateway(sendProductStateFunction: lambda.Function): void {
    const api = new apigateway.RestApi(this, 'ProductStateApi', {
      restApiName: 'Product State API',
      description: 'API for sending product state events.',
    });

    const productStateResource = api.root.addResource('product-state');
    productStateResource.addMethod('POST', new apigateway.LambdaIntegration(sendProductStateFunction), {
      methodResponses: [
        { statusCode: '200' },
        { statusCode: '400' },
        { statusCode: '500' },
      ],
    });

    sendProductStateFunction.grantInvoke(new iam.ServicePrincipal('apigateway.amazonaws.com'));
  }

  private grantPermissions(eventHistoryTable: dynamodb.Table, storeEventFunction: lambda.Function, sendProductStateFunction: lambda.Function): void {
    eventHistoryTable.grantReadWriteData(storeEventFunction);
    sendProductStateFunction.grantInvoke(new iam.ServicePrincipal('apigateway.amazonaws.com'));
  }

  private createEventBus(): events.EventBus {
    return new events.EventBus(this, EVENT_BUS_NAME, {
      eventBusName: EVENT_BUS_NAME,
    });
  }

  private createEventBridgeRules(
    eventBus: events.EventBus,
    storeEventFunction: lambda.Function,
    updateProductStateFunction: lambda.Function,
    forwardToStateMachineFunction: lambda.Function,
    dlq: sqs.Queue
  ): void {
    // Rule for triggering the on NewEventFromUser
    const newEventFromUserRule = new events.Rule(this, 'NewEventFromUserRule', {
      eventBus: eventBus,
      eventPattern: {
        source: ['your.event.source'], // Replace with your event source
        detailType: ['your.event.detailType'], // Replace with your event detail type
      },
    });

    newEventFromUserRule.addTarget(new targets.LambdaFunction(storeEventFunction, {
      retryAttempts: 3,
      maxEventAge: cdk.Duration.seconds(60),
      deadLetterQueue: dlq,
    }));

    newEventFromUserRule.addTarget(new targets.LambdaFunction(updateProductStateFunction, {
      retryAttempts: 3,
      maxEventAge: cdk.Duration.seconds(60),
      deadLetterQueue: dlq,
    }));

    newEventFromUserRule.addTarget(new targets.LambdaFunction(forwardToStateMachineFunction, {
      retryAttempts: 3,
      maxEventAge: cdk.Duration.seconds(60),
      deadLetterQueue: dlq,
    }));
  }

  private createRdsDatabase(): rds.DatabaseInstance {
    const vpc = new ec2.Vpc(this, 'Vpc', { maxAzs: 2 });

    return new rds.DatabaseInstance(this, 'ProductStateDatabase', {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_15,
      }),
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
      vpc,
      multiAz: false,
      allocatedStorage: 20,
      maxAllocatedStorage: 100,
      databaseName: 'ProductStateDB',
      credentials: rds.Credentials.fromGeneratedSecret('postgres'),
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      deletionProtection: false,
    });
  }

  private createStateMachine(updateProductStateFunction: lambda.Function): sfn.StateMachine {
    const updateStateTask = new sfnTasks.LambdaInvoke(this, 'UpdateProductStateTask', {
      lambdaFunction: updateProductStateFunction,
      outputPath: '$.Payload',
    });

    const checkIfFinished = new sfn.Choice(this, 'CheckIfFinished')
      // If event equals Finished, run the update function once more and then end the state machine
      .when(
        sfn.Condition.stringEquals('$.eventType', 'Finished'),
        new sfnTasks.LambdaInvoke(this, 'UpdateProductStateTaskAgain', {
          lambdaFunction: updateProductStateFunction,
          outputPath: '$.Payload',
        }).next(new sfn.Succeed(this, 'FinishedStateMachine'))
      )
      // If event equals Failed, log the error and end the state machine
      .when(
        sfn.Condition.stringEquals('$.eventType', 'Failed'),
        new sfn.Fail(this, 'FailedStateMachine', {
          error: 'ProductStateError',
          cause: 'The product state update failed.',
        })
      )
      .otherwise(updateStateTask);

    const definition = updateStateTask.next(checkIfFinished);

    return new sfn.StateMachine(this, 'ProductStateMachine', {
      definition,
      stateMachineType: sfn.StateMachineType.STANDARD,
    });
  }
}