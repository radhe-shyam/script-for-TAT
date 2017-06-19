const mongodb = require("mongodb");
const dbURI = require('./db-uri.json')[0];
const dbPromise = mongodb.MongoClient.connect(dbURI);
const fs = require('fs');
const bluebird = require('bluebird');
const X = require('xlsx');

function to_json(file) {
	var result = [];
	console.log('Working on : ', file);
	const workbook = X.readFile(file);
	workbook.SheetNames.forEach(function (sheetName) {
		var roa = X.utils.sheet_to_row_object_array(workbook.Sheets[sheetName]);
		if (roa.length > 0) {
			roa.forEach((row, key)=> {
				let record = {
					id: key,
					origin: (row['Origin Pincode'] || '').toUpperCase(),
					destination: (row['Destination Pincode'] || '').toUpperCase(),
					serviceType: (row['Service Type'] || '')
				};
				// console.log(record);	
				result.push(record);
			});
		}
	});
	return result;
}

bluebird.coroutine(function*() {
	let startTime = new Date().valueOf();
	const db = yield dbPromise;
	console.log('Data uploading to => ', dbURI);
	let files = ['./radhe.xlsx'];

		const data = files.map(file=>to_json(file))
			.reduce(function (prev, curr) {
				return curr.concat(prev);
			}, []);
		console.log('Data parsing completed.');
		bluebird.map( data, record => {
			let query = {
				pincode: record.origin
			};
			console.log(JSON.stringify(query));
			return db.collection('serviceablePincode').findOne(query)
			.then( _ => {
				if ( _ ) {
					record.originDetails = _;
					record.city = _.destinationBranchCity;
					query = {
						city: record.originDetails.destinationBranchCity,
						pincode: record.destination,
						serviceType: record.serviceType
					};
					console.log(JSON.stringify(query));
					return db.collection('TAT').findOne(query);
				} else {
					Promise.reject('pincode not found.');
				}
			})
			.then( _ => {
				if ( _ ) {
					record.TATData = _;
					record.TAT = _.TAT;
					console.log(JSON.stringify(record));
				} else {
					return Promise.reject('TAT not found.');
				}
			})
			.catch(e => {
				console.log(JSON.stringify(record));
				record.reason = e.toString();
				console.log(record.reason);
			})
		}, { concurrency: 100})
		.then( _ => {
			let fileData = '"Origin Pincode","Service Type","Destination Pincode","TAT"\n';
			data.forEach(record => {
				fileData += record.origin + ',' + record.serviceType + ',' 
				+ record.destination + ',' + (record.TAT || '') + ',"' + (record.reason || '') + '"' + '\n';
			});
			fs.writeFile('./output(' + new Date() + ').csv',fileData, (err, data) => {
				if (err) {
					console.log(err);
				} else {
					console.log('Script completed successfully');
				}
				console.log('Total Time Taken (in seconds) => ', (new Date().valueOf() - startTime) / 1000);
				process.exit(0);
			})
		});
})();