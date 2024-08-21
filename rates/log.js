const dotenv = require('dotenv').config({path : "../configuration/.env"})
const sql = require('mssql');
const config = require('../configuration/config.js').config
const ExecuteQuery = require('../db/ExecuteQuery.js').ExecuteQuery


exports.insertLog = async function insertLog(plateform = null, step = null, environment = null, fileName = null, size = null, status = null, comment = null, duration = null) {
    
    console.log(new Date(),"Logging", fileName ? "for '" + fileName + "'" : "", step ?  "Step : " + step : "", "..." )

    let query = `

    INSERT into logs_schedulerPush(
        [timestamp]
        ,[plateform]
        ,[step]
        ,[environment]
        ,[fileName]
        ,[size] 
        ,[status]
        ,[comment]
        ,[duration]
    )

    select
     current_timestamp
    ,'${plateform}'
    ,'${step}'
    ,'${environment}'
    ,'${fileName}'
    ,'${JSON.stringify(size)}'
    ,'${status}'
    ,'${JSON.stringify(comment)}'
    ,'${duration}'
    
    `;

    await ExecuteQuery(query)

   

};