self.importScripts('../bower_components/aws-sdk-js/dist/aws-sdk.js');

const KEY = "vlad-work";

const BUCKET = "l2it-bucket";
const TOKEN_COGNITO = "us-east-1:2114898a-71dd-43c4-9bb6-1ea85db7baa4";

request = {};
batchReady = false;
params = [];
s3 = {};
window = {};

onmessage = function (e) {
  console.log('Files received from main script');
  let file = e.data[0];
  _newBatch(file);
}

function _newBatch(file) {
  AWS.config.update({
    region: 'US-EAST-1',
    credentials: new AWS.CognitoIdentityCredentials({
      IdentityPoolId: TOKEN_COGNITO
    }),
    useAccelerateEndpoint: true
  });

  this.s3 = new AWS.S3({
    apiVersion: '2006-03-01',
    params: {
      region: 'US-EAST-1',
      Bucket: BUCKET
    }
  });
  this.params = {
    Bucket: BUCKET,
    Key: KEY
  }
  this.batchReady = false;
  this.request = this.s3.createMultipartUpload(this.params, function (err, data) {
    if (err) {
      console.log(err, err.stack);
    } else {
      this._uploadeFiles(file);
    }
  }.bind(this));
}
