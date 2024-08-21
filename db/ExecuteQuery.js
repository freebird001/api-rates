const dotenv = require('dotenv').config({path : "../configuration/.env"})
const sql = require('mssql');
const config = require('../configuration/config.js').config

exports.ExecuteQuery = async function ExecuteQuery(query) {
    var dbConn = new sql.ConnectionPool(config.db_config);

    return await dbConn.connect(config.db_config).then(async function() {
        var requests = new sql.Request(dbConn);

        return await requests.query(query).then(async(recordset) => {

            dbConn.close();
            sql.close();

            return recordset.recordsets


        }).catch(async function(err) {

            console.log("Error : Database Execution", err, "\n\n")
            dbConn.close();
            sql.close();

        });

    }).catch(async function(err) {

        console.log("Error : Database Connexion", err, "\n\n")
        dbConn.close();
        sql.close();

    });

};