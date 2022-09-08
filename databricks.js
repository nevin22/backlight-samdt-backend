const schedule = require("node-schedule");
const { DBSQLClient } = require("@databricks/sql");

var token = process.env.DATABRICKS_TOKEN;
var server_hostname = process.env.DATABRICKS_SERVER_HOSTNAME;
var http_path = process.env.DATABRICKS_HTTP_PATH;

if (!token || !server_hostname || !http_path) {
  throw new Error(
    "Cannot find Server Hostname, HTTP Path, or personal access token. " +
      "Check the environment variables DATABRICKS_TOKEN, " +
      "DATABRICKS_SERVER_HOSTNAME, and DATABRICKS_HTTP_PATH."
  );
}

const client = new DBSQLClient();
const utils  = DBSQLClient.utils;

let restart_count = 0;
// let recent = '2022-07-05T12:05:37.938+0000';

let initiate_connection = () => {
  client
    .connect(
      (options = {
        token: token,
        host: server_hostname,
        path: http_path,
      })
    )
    .then(async (client) => {
      console.log("Databrix Connection established");
      session = await client.openSession();
      // schedule.scheduleJob('*/5 * * * * *', async () => {
      //   session = await client.openSession();
      //   if (global_allow_snapper_execution) {
      //     console.log(`Processing new batch - restarted ${restart_count} times`);
      //     snapshot_queue();
      //   } else {
      //     console.log('Queue skipped.. previous data status update still on progress')
      //   }
      // });
    })
    .catch((error) => {
      console.log("Connect error: ", error.message);
    });
};

client.on("error", async (error) => {
  console.log("you should retry", error);
  initiate_connection();
});

const execute_query = async (query_str) => {
  const session = await client.openSession();
  const queryOperation = await session.executeStatement(query_str, {
    runAsync: true,
  });

  await utils.waitUntilReady(
    (operation = queryOperation),
    (progress = false),
    (callback = () => {})
  );

  await utils.fetchAll((operation = queryOperation));

  await queryOperation.close();

  const result = utils.getResult((operation = queryOperation)).getValue();
  
  session.close();
  return result;
};

// client.on('error', async (error) => {
//   client.close();
//   setTimeout(() => {
//     restart_count += 1;
//     global_allow_snapper_execution = true;
//     initiate_connection();
//   }, 2000)
//   console.log('you should retry', error);
// });

// const execute_query = async (query) => {
//   return new Promise(async (resolve, reject) => {
//     const client = new DBSQLClient();
//     const utils  = DBSQLClient.utils;
//     let session = null;
//     client.on('error', async (error) => {
//       global_allow_snapper_execution = true;
//       console.log('you should retry', error);
//     });

//     await client.connect(
//       options = {
//         token: token,
//         host:  server_hostname,
//         path:  http_path
//       })
//     .then(async function (client) {
//       console.log(query.substring(0, 50));
//       session = await client.openSession();
//       console.log(1);
//       const queryOperation = await session.executeStatement(statement = query, options = { runAsync: true });
//       console.log(2);
//       await utils.waitUntilReady(
//         operation = queryOperation,
//         progress = false,
//         callback = () => { }
//       );

//       console.log(3);
//       await utils.fetchAll(
//         operation = queryOperation
//       );
//       await queryOperation.close();
//       const result = utils.getResult(
//         operation = queryOperation
//       ).getValue();

//       console.log('result success');
//       return resolve(result);
//     }).catch(error => {
//       console.log('Connect error: ', error.message);
//       return reject(error)
//     }).finally(async () => {
//       await session.close();
//       await client.close();
//     })
//   })
// }

// const execute_query = async (query) => {
// console.log(query.substring(0,50));
//     try {
//       console.log(1);
//       const queryOperation = await session.executeStatement(statement = query, options   = { runAsync: true });
//       console.log(2);
//       await utils.waitUntilReady(
//         operation = queryOperation,
//         progress  = false,
//         callback  = () => {}
//       );
//       console.log(3);
//       await utils.fetchAll(
//         operation = queryOperation
//       );
//       console.log(4);
//       await queryOperation.close();
//       console.log(5);
//       const result = utils.getResult(
//         operation = queryOperation
//       ).getValue();

//       await session.close();
//       return result
//     } catch (error) {
//       console.log('execute_error', error);
//       return error
//     }
// }

exports.execute_query = execute_query;
exports.initiate_connection = initiate_connection;
