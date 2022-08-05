require('dotenv').config();
const fs = require('fs');
const moment = require('moment');
var CERT = fs.readFileSync('./ca.pem');

const mqtt = require('mqtt');
const mqttClient = mqtt.connect(process.env.mqttHost, { clientId: 'mqttjs_' + Math.random().toString(16).substr(2, 8), ca: CERT });

const db = require('./database/viana');
const { QueryTypes } = require('sequelize');

const { EventHubProducerClient } = require("@azure/event-hubs");
const AZURE_EVENT_HUB_CONNECTION_STRING = process.env.AZURE_EVENT_HUB_CONNECTION_STRING;
const AZURE_EVENT_HUB_NAME = process.env.AZURE_EVENT_HUB_NAME;
const producer = new EventHubProducerClient(AZURE_EVENT_HUB_CONNECTION_STRING, AZURE_EVENT_HUB_NAME);
let gate = [];

mqttClient.on('close', () => {
  console.log(`mqtt client disconnected`);
  mqttClient.reconnect();
});

mqttClient.on('reconnect', () => {
  console.log(`mqtt client reconnected`);
});

mqttClient.on('error', (err) => {
  console.log('mqtt error ', err);
  mqttClient.reconnect();
});

mqttClient.on('connect', () => {
  console.log(`mqtt client connected`);
  db.postgres.query(
    `
      select ms.type, s.id, s.content ->> 'api_key' as api_key, s.content ->> 'serial_id' as serial_id, s.content as sensor, sa.content -> 'scene' as scene from sensors s
      inner join samdt sa on s.id = CAST (sa."content" ->> 'feeder_id' AS INTEGER)
      inner join mqtt_sensors ms on ms.id = sa.id
      where s."content" ->> 'serial_id' in (select content ->> 'serial_id' as serial_id from mqtt_sensors)
    `,
    {
      raw: true,
      type: QueryTypes.SELECT,
    }
  ).then(async (result) => {
    global_mqtt_sensors_list = result;
    mqttClient.subscribe('/merakimv/+/custom_analytics');

    // mqttClient.subscribe('/merakimv/Q2TV-ND7F-9DHJ/custom_analytics');
    // mqttClient.subscribe('/merakimv/Q2TV-9PBP-ZFY3/custom_analytics');
    // mqttClient.subscribe('/merakimv/Q2MV-RLRY-Q5HY/custom_analytics');
    // mqttClient.subscribe('/merakimv/Q2MV-GTGY-PB5D/custom_analytics');
    // mqttClient.subscribe('/merakimv/Q2MV-FVHP-5QKB/custom_analytics');
    // mqttClient.subscribe('/merakimv/Q2JV-H5GQ-JBM2/custom_analytics');
  })
  .catch((err) => {
    console.log("error fetching sensor details");
  });
})

mqttClient.on('message', async (topic, message) => {
  if (global_database_connected) {
    let mqtt_data = JSON.parse(message.toString());
    let edtHour = moment(parseInt(mqtt_data.timestamp)).utc().hours() - 4;
    let serial_id = topic.split('/')[2];
    
    //check if topic is inside global_mqtt_sensors_list
    if (global_mqtt_sensors_list.find(s => s.serial_id === serial_id)) {
      if (mqtt_data && mqtt_data.outputs && mqtt_data.outputs.length > 0) {
        let serial_id = topic.split('/')[2];
        let index = global_mqtt_sensors_list.map(s => s.serial_id).indexOf(serial_id);
        let sensor_data = global_mqtt_sensors_list[index];

        if (sensor_data.type.name === 'Starbucks' && [1,2,3,21,22,23,24].includes(edtHour)) {
          console.log(`REJECTED - detection edt hour is ${edtHour} - ${moment(parseInt(mqtt_data.timestamp))}`);
        } else {
          if (!gate.find(i => i === serial_id)) {
            console.log(`SAVING - detection EDT hour is ${edtHour} - ${moment(parseInt(mqtt_data.timestamp))}`);
            gate.push(serial_id)
            db.mqtt_detections.create({
              serial_id,
              api_key: sensor_data.api_key,
              snapshot_generated: false,
              track_object: mqtt_data.outputs,
              timestamp_str: mqtt_data.timestamp,
              processing: false,
              timestamp_date: moment(mqtt_data.timestamp).toISOString(),
              type: {
                name: sensor_data.type
              }
            })
            .then(res => {
              setTimeout(() => {
                gate.splice(gate.indexOf(serial_id), 1);
              }, 10000)
            })
          }
        }
      } else {
        console.log(topic, ' - message has no output')
      }
    }
  }
})


module.exports = mqttClient;