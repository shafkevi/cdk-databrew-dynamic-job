const AWS = require('aws-sdk');

const dataBrew = new AWS.DataBrew();

function uuidv4() {
    var dt = new Date().getTime();
    var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = (dt + Math.random()*16)%16 | 0;
        dt = Math.floor(dt/16);
        return (c=='x' ? r :(r&0x3|0x8)).toString(16);
    });
    return uuid;
}

exports.handler = async(event, context) => {

    console.log(JSON.stringify(event, null, 2));
    /* 
        TODO:
            Error handling
            Configure options for more than just CSVs.
    */
    const {
        jobId,
        format,
        inputBucket,
        inputKey,
        outputBucket,
        outputKey,
        recipeName,
        recipeVersion,
    } = event.inputs;

    const uniqueJobId = jobId || uuidv4();

    const datasetName = `dataSet-${uniqueJobId}`;
    const datasetParams = {
        Name: datasetName,
        Format: format,
        Input: {
            S3InputDefinition: {
                Bucket: inputBucket,
                Key: inputKey
            }
        },
        FormatOptions: {
            Csv: {
                Delimiter: ",",
                HeaderRow: true,
            }
        }
    };
    console.log(JSON.stringify(datasetParams));
    console.log('Creating Dataset');
    const dataset = await dataBrew.createDataset(datasetParams).promise();
    console.log(dataset);

    
    const recipeJobName = `recipeJob-${uniqueJobId}`;
    const recipeJobParams = {
        Name: recipeJobName,
        DatasetName: datasetName,
        RoleArn: process.env.ROLE,
        Outputs: [
            {
                Location: {
                    Bucket: outputBucket,
                    Key: outputKey,
                },
                Format: format,
                FormatOptions: {
                    Csv: {
                        Delimiter: ","
                    }
                }, PartitionColumns: []
            }
        ],
        RecipeReference: {
            Name: recipeName,
            RecipeVersion: recipeVersion, 
        },
        LogSubscription: "ENABLE",
    }
    console.log(JSON.stringify(recipeJobParams));
    console.log('Creating Recipe Job');
    const recipeJob = await dataBrew.createRecipeJob(recipeJobParams).promise();
    console.log(recipeJob);


    return {
        recipeJobName,
        datasetName,
        outputBucket,
        outputKey,
    }
}