const path = require('path');
import { CfnOutput, Construct, Aws } from "@aws-cdk/core";
import { PolicyStatement, Role, ServicePrincipal } from '@aws-cdk/aws-iam';
import { GlueDataBrewStartJobRun } from "@aws-cdk/aws-stepfunctions-tasks";
import { IntegrationPattern, JsonPath, StateMachine, CustomState, Chain } from "@aws-cdk/aws-stepfunctions";

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

    const createDatasetJson = {
      Type: "Task",
      Parameters: {
        "Input": {
          "S3InputDefinition": {
            "Bucket.$": "$.inputBucket",
            "Key.$": "$.inputKey"
          }
        },
        "FormatOptions": {
          "Csv": {
            "Delimiter": ",",
            "HeaderRow": true
          }
        },
        "Name.$": "States.Format('dataSet-{}',$.jobId)",
        "Format.$": "$.format"
      },
      Resource: "arn:aws:states:::aws-sdk:databrew:createDataset",
      ResultPath: "$.dataSet"
    };
    const createDatasetState = new CustomState(this, 'createDatasetState', {
      stateJson: createDatasetJson,
    });


    const createRecipeJobJson = {
      "Type": "Task",
      "Parameters": {
        "Name.$": "States.Format('recipeJob-{}',$.jobId)",
        "DatasetName.$": "States.Format('dataSet-{}',$.jobId)",
        "RoleArn": dataBrewExecutionRole.roleArn,
        "Outputs": [
          {
            "Location": {
              "Bucket.$": "$.outputBucket",
              "Key.$": "$.outputKey"
            },
            "Format.$": "$.format",
            "FormatOptions": {
              "Csv": {
                "Delimiter": ","
              }
            },
            "PartitionColumns": []
          }
        ],
        "RecipeReference": {
          "Name.$": "$.recipeName",
          "RecipeVersion.$": "$.recipeVersion"
        },
        "LogSubscription": "ENABLE"
      },
      "Resource": "arn:aws:states:::aws-sdk:databrew:createRecipeJob",
      "ResultPath": "$.createRecipeJob"
    }
    const createRecipeJobState = new CustomState(this, 'createRecipeJobState', {
      stateJson: createRecipeJobJson,
    });

    const databrewTask = new GlueDataBrewStartJobRun(this, "DataBrewJob", {
      integrationPattern: IntegrationPattern.RUN_JOB,
      name: JsonPath.stringAt("$.createRecipeJob.Name"),
      resultPath: "$.execution",
    });

    const deleteJobJson = {
      "Type": "Task",
      "Parameters": {
        "Name.$": "States.Format('recipeJob-{}',$.jobId)"
      },
      "Resource": "arn:aws:states:::aws-sdk:databrew:deleteJob",
      "ResultPath": "$.deleteJob"
    }
    const deleteJobState = new CustomState(this, 'deleteJobState', {
      stateJson: deleteJobJson,
    });

    const deleteDatasetJson = {
      "Type": "Task",
      "Parameters": {
        "Name.$": "States.Format('dataSet-{}',$.jobId)"
      },
      "Resource": "arn:aws:states:::aws-sdk:databrew:deleteDataset",
      "ResultPath": "$.deleteDataSet"
    }
    const deleteDatasetState = new CustomState(this, 'deleteDatasetState', {
      stateJson: deleteDatasetJson,
    });


    const stateChain = Chain.start(createDatasetState)
      .next(createRecipeJobState)
      .next(databrewTask)
      .next(deleteJobState)
      .next(deleteDatasetState);


    const stateMachine = new StateMachine(this, "StateMachine", {
      definition: stateChain,
    });

    stateMachine.addToRolePolicy(new PolicyStatement({
      actions: ['iam:PassRole'], 
      resources: [dataBrewExecutionRole.roleArn]
    }));

    stateMachine.addToRolePolicy(new PolicyStatement({
      actions: [
        'databrew:CreateDataset',
        'databrew:CreateRecipeJob',
        'databrew:DeleteJob',
        'databrew:DeleteDataset'
      ], 
      resources: [`arn:aws:databrew:${Aws.REGION}:${Aws.ACCOUNT_ID}:*`]
    }));

  }
}
