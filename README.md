# migrate mongoDB to S3 / R2 nodejs example

This is an example for mongodb GridFS bucket data stream upload to S3 or R2.

### Usage
1. get your `ACCOUNT_ID`, `ACCESS_KEY_ID`, `ACCESS_KEY` from S3 ro R2.
2. reset `.env` by `.env.example`.

### package.js
```json
"dependencies": {
    "@aws-sdk/client-s3": "^3.342.0",
    "@aws-sdk/lib-storage": "^3.342.0",
    "dotenv": "^16.1.3",
    "mongodb": "^5.5.0"
  }
```