const path = require('path');
import { CfnOutput, Construct } from "@aws-cdk/core";
import { PolicyStatement, Role, ServicePrincipal } from '@aws-cdk/aws-iam';
import { Function, Runtime, Code } from '@aws-cdk/aws-lambda';
import { GlueDataBrewStartJobRun, LambdaInvoke, } from "@aws-cdk/aws-stepfunctions-tasks";
import { Aws } from "@aws-cdk/core";
import { IntegrationPattern, JsonPath, StateMachine } from "@aws-cdk/aws-stepfunctions";

export interface DynamicRunnerProps { 

}

export default class DynamicRunner extends Construct {
  constructor(scope: Construct, id: string, props: DynamicRunnerProps) {
    super(scope, id);

    // Still a little too open of a role, but can be narrowed down
    // Based on your specific use case, especially the s3 permissions.
    const dataBrewExecutionRole = new Role(this, "DataBrewExecutionRole", {
      assumedBy: new ServicePrincipal("databrew.amazonaws.com"),
    })
    dataBrewExecutionRole.addToPolicy(new PolicyStatement({
      actions: ['*'], 
      resources: [
        `arn:aws:databrew:${Aws.REGION}:${Aws.ACCOUNT_ID}:*`,
        `arn:aws:s3:::*`,
        `arn:aws:logs:${Aws.REGION}:${Aws.ACCOUNT_ID}:*`,
      ]
    }));

    const setupFunction = new Function(this, "SetupFunction", {
      runtime: Runtime.NODEJS_14_X,
      handler: "index.handler",
      code: Code.fromAsset(path.join(__dirname, '../../src/setup')),
      environment: {
        "ROLE": dataBrewExecutionRole.roleArn
      }
    });
    setupFunction.addToRolePolicy(new PolicyStatement({
      actions: ['*'], 
      resources: [`arn:aws:databrew:${Aws.REGION}:${Aws.ACCOUNT_ID}:*`]
    }));
    setupFunction.addToRolePolicy(new PolicyStatement({
      actions: ['iam:PassRole'], 
      resources: [dataBrewExecutionRole.roleArn]
    }));
    const setupTask = new LambdaInvoke(this, "SetupLambdaTask", {
      lambdaFunction: setupFunction,
      payloadResponseOnly: true,
      resultPath: "$.setup",
    });


    const cleanupFunction = new Function(this, "CleanupFunction", {
      runtime: Runtime.NODEJS_14_X,
      handler: "index.handler",
      code: Code.fromAsset(path.join(__dirname, '../../src/cleanup')),
    });
    cleanupFunction.addToRolePolicy(new PolicyStatement({
      actions: ['*'], 
      resources: [`arn:aws:databrew:${Aws.REGION}:${Aws.ACCOUNT_ID}:*`]
    }));
    const cleanupTask = new LambdaInvoke(this, "CleanupLambdaTask", {
      lambdaFunction: cleanupFunction,
      payloadResponseOnly: true,
      resultPath: "$.cleanup",
    });


    const databrewTask = new GlueDataBrewStartJobRun(this, "DataBrewJob", {
      integrationPattern: IntegrationPattern.RUN_JOB,
      name: JsonPath.stringAt("$.setup.recipeJobName"),
      resultPath: "$.execution",
    });

    const definition = setupTask
      .next(databrewTask)
      .next(cleanupTask);

    const stateMachine = new StateMachine(this, "StateMachine", {
      definition,
    });


  }
}
