const Sequelize = require('sequelize');
const path = require("path");
const qs = require('querystring');
var fs = require('fs');
var db = {};
require('dotenv').config();
const schedule = require('node-schedule');
const starbucks_snapshotqueue = require('../snapshot_queues/starbucks_snapshotqueue');
const pandora_snapshotqueue = require('../snapshot_queues/pandora_snapshotqueue');

const conn = qs.parse(process.env.postgresConnection, ' ', '=');

console.log(conn);
const postgresConnection = new Sequelize(
    conn.dbname, 
    conn.user, 
    conn.password, 
    {
        // host: conn.host,
        port: conn.port,
        dialect: 'postgres',
        logging: false,
        dialectOptions: {
            ssl: true,
            connectTimeout: 1000,
            timezone: '+0800', //for writing to database 
            // put ssl support here | important in production
            // ssl: {
            //     key: cKey, 
            //     cert: cCert,
            //     ca: cCA
            // }
        },
        replication: {
          read: [
            { host: conn.host }
          ],
          write: { host: conn.host }
        },
        pool: {
          max: isNaN(parseInt(process.env.connection_pool_size)) ? 10 : parseInt(process.env.connection_pool_size),
          idle: 30000
        },
    }
);

postgresConnection
  .authenticate()
  .then(() => {
    schedule.scheduleJob('*/10 * * * * *', starbucks_snapshotqueue);
    schedule.scheduleJob('*/10 * * * * *', pandora_snapshotqueue);
    global_database_connected = true;
    console.log('Connection to PostgreSQL has been established successfully.');
  })
  .catch(err => {
    console.error('Unable to connect to PostgreSQL database:', err);
  });

let models = [];
let dir = './models/postgresql/';
let directory = path.join(__dirname, dir);

(async function () {
  fs.readdir(directory, function (err, files) {
    if (err) {
      console.error("Could not list the directory.", err);  
    }

    // for each file in folder
    files.forEach(function (file, index) {
      var file_name_without_ext = file.replace(/\.[^/.]+$/, "");
      models.push(require(dir + file_name_without_ext))
    });

    // for each model
    models.forEach(model => {
      const pgModel = model(postgresConnection, Sequelize, false)
      db[pgModel.name] = pgModel
    })

    Object.keys(db).forEach(key => {
      if ('associate' in db[key]) {
          db[key].associate(db)
      }
    })

    db.postgres = postgresConnection
    db.Sequelize = Sequelize
  });
})();

module.exports = db