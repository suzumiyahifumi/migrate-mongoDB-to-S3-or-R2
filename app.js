/* jshint esversion: 9 */
(async () => {

	const dotenv = require('dotenv');
	dotenv.config();
	// R2 have S3 API compatibility, import '@aws-sdk/client-s3'.
	const { S3Client } = require('@aws-sdk/client-s3');
	// use '@aws-sdk/lib-storage' to support stream upload.
	const { Upload } = require('@aws-sdk/lib-storage');
	// mongodb official driver
	const mongodb = require('mongodb');
	const MongoClient = require('mongodb').MongoClient;
	
	const dbUrl = process.env.DATA_STORE;

	const mongoConnect = async (o) => {
		try {
			let mongo = await MongoClient.connect(o.url || dbUrl);
			let database = mongo.db(o.dbName);
			let GridFS_Files = database.collection(`${o.bucketName}.files`);
			let GridFS_Chunks = database.collection(`${o.bucketName}.chunks`);
			return {
				db: database,
				GridFS_Files,
				GridFS_Chunks
			};
		}
		catch(error){
			console.log(error);
		}
	};

	const s3 = new S3Client({
		region: "auto",
		endpoint: `https://${process.env.S3_ro_R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
		credentials: {
			accessKeyId: `${process.env.S3_ro_R2_ACCESS_KEY_ID}`,
			secretAccessKey: `${process.env.S3_ro_R2_SECRET_ACCESS_KEY}`,
		}
	});

	const migrate = async (o) => {
		return new Promise(async (resolve, reject) => {
			const {
				db,
				GridFS_Files
			} = await mongoConnect(o);

			// something you want to check from GridFS_Files ro get mimetype from data.
			let FILE = await GridFS_Files.findOne({
				"metadata.hasR2": { $exists: false }
			});
			console.log("processing file: ",FILE);
			
			if(FILE == undefined) return resolve({msg: 'no data.', error: false, done: true});


			let _id = FILE._id;
			let id = FILE._id.toString();

			// get data from GridFS bucket.
			let bucket = new mongodb.GridFSBucket(db, {
				bucketName: o.bucketName
			});
			// create a download stream with mongodb lib.
			let downloadStream = await bucket.openDownloadStream(_id);

			// create Upload instance with "@aws-sdk/lib-storage".
			// some setting need to setup by yourself.
			let s3up = new Upload({
				client: s3,
				params: {
					Bucket: o.bucketName,
					Key: id,
					Body: downloadStream, // passthrough mongodb downloadStream directly.
					ContentType: FILE.metadata.mimeType
				}
			});
			
			try{
				// start upload to R2 or S3.
				let uploadDone = await s3up.done();

				// set GridFS_Files a hasR2 flag.
				let fileDoc = await GridFS_Files.updateOne({
					_id: _id
				}, {
					$set: {
						metadata: {
							hasR2: true,
							...FILE.metadata
						}
					}
				});

				// close db.
				db.close();

				resolve({msg: `data [ ${FILE.metadata.fileName}#${id} ] has uploaded.`, error: false, done: false});
			}
			catch(error){
				reject({msg: error.message, error: true, done: false});
			}
		});
	};

	try{
		while(true){
			let main = await migrate({
				dbName: "database_name",
				bucketName: "bucket_name"
			});
			if(main.error) {
				console.log(`[[Error]]`, main.msg);
				continue;
			}
			console.log(`[[Message]]`, main.msg);
			if(main.done) break;
		}
		console.log(`all process has done.`);
	}
	catch(error) {
		console.log("run error:", error);
	}
}).call(this);