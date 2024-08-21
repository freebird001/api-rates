"use strict";
const dotenv = require('dotenv').config({path : "../configuration/.env"})
var axios = require('axios');
const fs = require('fs')
const {json} = require('body-parser');
const {sizeof} = require("file-sizeof");
const compressing = require('compressing')
const compression = require('compression')
const { createReadStream, createWriteStream } = require('fs')
//const { createGzip } = require('zlib')
const configs = require('../configuration/config.js').config
const insertLog = require('./log.js').insertLog;
const ExecuteQuery = require('../db/ExecuteQuery.js').ExecuteQuery;
const env = configs.env
const SRC_EXTENSION = ".json"
const DEST_EXTENSION = ".json.gz"
const FILE_BASE_NAME = `${env} Data file J+`
const DATAFILE_URL = `./DataFiles/${FILE_BASE_NAME}`

module.exports = async function(context, myTimer) {

    await main()

};

const compressFile = async(path) => {
    const handleStream = createReadStream(path)
    handleStream
      .pipe(createGzip())
      .pipe(createWriteStream(`${path}.gz`))
      .on('error', function(err){ 
        console.log(new Date(),`Compression process Failed: ************************* ${path}`,  err.stack)
        return
      })
      .on('finish', async() => {
        console.log(new Date(), `Compression process from ${path} => ${path}.gz completed. Success`, "...")
        await sleep(10000)
      })
}

async function main(){

    var startTime = new Date().getTime();
    var elapsedTime = 0;

    if(!configs.db_config.server){
        console.log('Please run on specific env : npm run push-dev-rate or push-preprod-rate or push-prod-rate ')
        return
    }

    console.log('Execution Environement : ', env)       
    //insertLog('CARGOAI', 'Begin', env, null, null, null, null, null);
    var jobTime = new Date().getTime();
    var totalExecutionTime = 0

    var reqs = ` select top(10) * from price`

    var startTime = new Date().getTime();
    var elapsedTime = 0;

    var startConnectionTime = new Date().getTime();
    var EndConnectionTime = 0;

    console.log("\n")

    var data = await getData(reqs)

    if(!data) return 

    EndConnectionTime = new Date().getTime() - startConnectionTime;

    //insertLog('CARGOAI', 'Database connexion n°' + i, env, null, null, null, null, EndConnectionTime / 1000);


    return console.log(data)

    //var response = await maps(data)

    var startFileCreationTime = new Date().getTime();
    var EndFileCreationTime = 0;

    const FILE_TO_CREATE = DATAFILE_URL + i + SRC_EXTENSION


    try {

        //FILE CREATION
        
        fs.writeFileSync(FILE_TO_CREATE, JSON.stringify(response),  {encoding: "utf8"})
        EndFileCreationTime = new Date().getTime() - startFileCreationTime;
    


        await sleep(10000)
    
        //insertLog('CARGOAI', 'File creation', env, FILE_TO_CREATE, sizeof.SI(FILE_TO_CREATE), 'OK', null, EndFileCreationTime / 1000)
        
        var startFileCompressionTime = new Date().getTime();
        var EndFileCompressionTime = 0;

        //FILE COMPRESSION
        await compressFile(FILE_TO_CREATE)

        EndFileCompressionTime = new Date().getTime() - startFileCompressionTime

        await sleep(10000)

        const FILE_TO_SEND = DATAFILE_URL + i + DEST_EXTENSION

        //insertLog('CARGOAI', 'File compression', env, FILE_TO_SEND, sizeof.SI(FILE_TO_SEND), 'OK', null, EndFileCompressionTime / 1000)

        await sleep(10000)

        var beginTime = new Date().getTime();
        
        //FILE UPLOAD
        await GetURLCAI(i, FILE_TO_SEND);

        var executionTime = new Date().getTime() - beginTime;

        elapsedTime = new Date().getTime() - startTime;
    
        await sleep(10000);

        const used = process.memoryUsage().heapUsed / 1024 / 1024;

        console.log(new Date(), "The script of Batch", i, "has used approximately", Math.round(used * 100) / 100, "MB", "of memory", "...");

        await sleep(10000);

        totalExecutionTime = new Date().getTime() - jobTime;
    
        //insertLog('CARGOAI', 'End', env, env, null, null, null, totalExecutionTime / 1000);

    } catch (error) {
        console.log(error)
        EndFileCreationTime = new Date().getTime() - startFileCreationTime;
        //await insertLog('CARGOAI', 'File creation', env, FILE_TO_CREATE, sizeof.SI(FILE_TO_CREATE), 'KO', error.toString(), EndFileCreationTime / 1000)
    }
         

}

async function getData(query) {
 let data = await ExecuteQuery(query)
 return data
}

async function maps(data) {

    return ({
        "routes": data[0].map((o) => {
            return {
                "id": o.routing_id,
                "legs": [
                    (o.cr_1 === null) ? null : {
                        "airlineCode": o.cr_1,
                        "flightNumber": o.flt_nr_1,
                        "departureTime": o.dep_loc_1,
                        "departureAirport": o.bpt_1,
                        "arrivalAirport": o.opt_1,
                        "arrivalTime": o.arv_loc_1,
                        "plane": o.aft_typ_1,
                        "airline": o.cr_1
                    },
                    (o.cr_2 === null) ? null : {
                        "airlineCode": o.cr_2,
                        "flightNumber": o.flt_nr_2,
                        "departureTime": o.dep_loc_2,
                        "departureAirport": o.bpt_2,
                        "arrivalAirport": o.opt_2,
                        "arrivalTime": o.arv_loc_2,
                        "plane": o.aft_typ_2,
                        "airline": o.cr_2
                    },
                    (o.cr_3 === null) ? null : {
                        "airlineCode": o.cr_3,
                        "flightNumber": o.flt_nr_3,
                        "departureTime": o.dep_loc_3,
                        "departureAirport": o.bpt_3,
                        "arrivalAirport": o.opt_3,
                        "arrivalTime": o.arv_loc_3,
                        "plane": o.aft_typ_3,
                        "airline": o.cr_3
                    },

                    (o.cr_4 === null) ? null : {
                        "airlineCode": o.cr_4,
                        "flightNumber": o.flt_nr_4,
                        "departureTime": o.dep_loc_4,
                        "departureAirport": o.bpt_4,
                        "arrivalAirport": o.opt_4,
                        "arrivalTime": o.arv_loc_4,
                        "plane": o.aft_typ_4,
                        "airline": o.cr_4
                    },

                    (o.cr_5 === null) ? null : {
                        "airlineCode": o.cr_5,
                        "flightNumber": o.flt_nr_5,
                        "departureTime": o.dep_loc_5,
                        "departureAirport": o.bpt_5,
                        "arrivalAirport": o.opt_5,
                        "arrivalTime": o.arv_loc_5,
                        "plane": o.aft_typ_5,
                        "airline": o.cr_5
                    }
                ],
                "airlineCode": o.carrier,
                "arrivalTime": o.arv_loc,
                "arrivalAirport": o.dest,
                "departureTime": o.dep_loc,
                "departureAirport": o.orig
            }
        })
    })
}

function jsonStrinfy(data){

    if(data && data.routes && data.routes.length > 500000){
        console.log('MAP 1')
        let routes = data.routes
        let stringify = "{ routes : [" + routes.map(r => JSON.stringify(r)).join(",") + "] }";

        return stringify

    }else{
        console.log('MAP 2')
        return JSON.stringify(data)
    }
}

async function GetURLCAI(i, FILE) {

    var startTime = new Date().getTime();
    var elapsedTime = 0;

    var date = new Date();
    date.setDate(date.getDate() + i)
    let y = date.getFullYear();
    let m = (date.getMonth() + 1);
    let d = (date.getDate());
    var dataDay = (y + "-" + (m < 10 ? "0" + m : m) + "-" + (d < 10 ? "0" + d : d))

    var config = {
        method: 'get',
        url: `${configs.endpoints.ENDPOINT_CARGOAI}schedule-url?day=${dataDay}`,
        headers: {
            'x-api-key': configs.endpoints.KEY_CARGOI
        }
    };

     axios(config).then(async function(response) {
        elapsedTime = new Date().getTime() - startTime;
        await sleep(10000)
        await uploadDataCAI(i, FILE, response); 
        await sleep(10000)

        insertLog('CARGOAI', 'Get URL', env, FILE, sizeof.SI(FILE), 'OK', response.data, elapsedTime / 1000)
    }).catch(async function(error) {
        elapsedTime = new Date().getTime() - startTime;
        insertLog('CARGOAI', 'Get URL', env, FILE, sizeof.SI(FILE), 'NOK', error, elapsedTime / 1000)
    });
}

async function uploadDataCAI(i, FILE, response) {
 
    var elapsedTime = 0;
    var SIGNED_URL = response.data.upload_url
    
    const payload = fs.createReadStream(FILE);

    var config = {
        method: 'put',
        url: SIGNED_URL,
        headers: {
            "Content-Type": "",
            "Content-Length": fs.statSync(FILE).size
        },
        data: payload,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
    };

    axios(config).then(function(response) {
        //console.log(JSON.stringify(response.data));
        insertLog('CARGOAI', 'Push file', env, FILE, sizeof.SI(FILE), 'OK', JSON.stringify(response.data), elapsedTime / 1000)
        console.log(new Date(), "Result for Batch", i, response && response.status !== 200 ? "Failed" : "Success", response.status,  "..." )
    }).catch(async function(error) {
        console.log(new Date(), "Result : " + i,  "Failed" , "\nError : " + error.toString() )
        insertLog('CARGOAI', 'Push file', env, FILE, sizeof.SI(FILE), 'KO', error, elapsedTime / 1000)
    });

}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function localExecute() {
    //console.log(config.endpoints)
    //return
    await main()
}

localExecute();