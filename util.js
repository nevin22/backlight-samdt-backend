const db = require('./database/viana');
const { QueryTypes } = require('sequelize');

const util = {};

util.update_global_mqtt_sensors_list = async () => {
  await db.postgres
  .query(
      `
      select ms.type, s.content ->> 'api_key' as api_key, s.id, s.content ->> 'serial_id' as serial_id, s.content as sensor, sa.content -> 'scene' as scene from sensors s
      inner join samdt sa on s.id = CAST (sa."content" ->> 'feeder_id' AS INTEGER)
      inner join mqtt_sensors ms on ms.id = sa.id
      where s."content" ->> 'serial_id' in (select content ->> 'serial_id' as serial_id from mqtt_sensors)
    `,
    {
      raw: true,
      type: QueryTypes.SELECT,
    }
  )
  .then(async (result) => {
    global_mqtt_sensors_list = result;
  })
  .catch((err) => {
    console.log("update global list err", err);
  });
}

module.exports = util;