const AWS = require('aws-sdk');

const dataBrew = new AWS.DataBrew();

exports.handler = async(event, context) => {

    console.log(JSON.stringify(event, null, 2));
    const {
        recipeJobName,
        datasetName,
    } = event.setup;

    /* TODO:
        Add some error handling
            1. See if job exists and only then delete
            2. See if dataset exists and only then delete
    */
    console.log('Deleting Job');
    const deleteJobResponse = await dataBrew.deleteJob({
        Name: recipeJobName
    }).promise();
    console.log(deleteJobResponse);

    console.log('Deleting Dataset');
    const deleteDatasetResponse = await dataBrew.deleteDataset({
        Name: datasetName
    }).promise();
    console.log(deleteDatasetResponse);

    return;
}