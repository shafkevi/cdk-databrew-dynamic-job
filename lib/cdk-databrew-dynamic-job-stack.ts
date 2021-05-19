import * as cdk from '@aws-cdk/core';
import DynamicRunner from "./constructs/DynamicRunner";

export class CdkDatabrewDynamicJobStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here

    new DynamicRunner(this, "DynamicRunner", {});
  }
}
