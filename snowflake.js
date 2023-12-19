require('dotenv').config();

const snowflake = require('snowflake-sdk');

// Snowflake connection parameters
const connectionOptions = {
  account: process.env.host,
  username: process.env.user,
  password: process.env.pass,
  warehouse: process.env.warehouse,
  database: process.env.database,
  schema: process.env.schema,
};

let snowflakeConnection = snowflake.createConnection(connectionOptions);

let initiate_connection = () => {
    snowflakeConnection.connect((err, conn) => {
      if (err) {
        console.error('Error connecting to Snowflake:', err);
      } else {
        console.log('Connected to snowflake with id', conn.getId())
      }
    });
}

let execute_query = (query, cb) => {
  let hehe = snowflakeConnection.execute({
    sqlText: query,
    complete: function(err, stmt, rows) {
      if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
        cb(true)
      } else {
        console.log(new Date(), '- Successfully executed statement');
        cb(false, rows)
      }
    }
  })
}

exports.execute_query = execute_query;
exports.initiate_connection = initiate_connection;