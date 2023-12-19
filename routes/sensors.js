var express = require("express");
var router = express.Router();
const { update_global_mqtt_sensors_list } = require("../util");
const db = require("../database/viana");
const { QueryTypes } = require("sequelize");

router.get("/list", async (req, res) => {
  let mqtt_sensors = await db.postgres
    .query(
      `select content ->> 'serial_id' as serial_id, type, id from mqtt_sensors`,
      {
        raw: true,
        type: QueryTypes.SELECT,
      }
    )
    .then(async (result) => {
      return result;
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message,
      });
    });

  let sensors = await db.postgres
    .query(
      `
        select content ->> 'serial_id' as serial_id, id
        from sensors
        where content ->> 'serial_id' not in (select content ->> 'serial_id' from mqtt_sensors)
      `,
      {
        raw: true,
        type: QueryTypes.SELECT,
      }
    )
    .then(async (result) => {
      return result;
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message,
      });
    });

  res.status(200).json({
    mqtt_sensors,
    sensors,
    types: ["Pandora", "Starbucks"],
    message: "Success",
  });
});

router.post("/delete", async (req, res) => {
  let to_delete = req.body.serial_id;
  await db.postgres
    .query(
      `DELETE from mqtt_sensors where content ->> 'serial_id' = '${to_delete}'`,
      {
        raw: true,
        type: QueryTypes.DELETE,
      }
    )
    .then(async (result) => {
      await db.postgres
        .query(
          `
            select content ->> 'serial_id' as serial_id, id
            from sensors
            where content ->> 'serial_id' not in (select content ->> 'serial_id' from mqtt_sensors)
          `,
          {
            raw: true,
            type: QueryTypes.SELECT,
          }
        )
        .then((result2) => {
          update_global_mqtt_sensors_list();
          res.status(200).json({
            sensors: result2,
            message: "Success",
          });
        });
    })
    .catch((err) => {
      console.log(err);
      res.status(500).json({
        message: err.message,
      });
    });
});

router.post("/update", async (req, res) => {
  let to_add = req.body;

  let sensor_details = await db.postgres
    .query(
      `SELECT * from sensors where content ->> 'serial_id' = '${to_add.serial_id}'`,
      {
        raw: true,
        type: QueryTypes.SELECT,
      }
    )
    .then((result) => {
      let data = result[0];

      return {
        id: data.id,
        content: data.content,
        type: to_add.type,
      };
    })
    .catch((err) => {
      console.log(err);
      res.status(500).json({
        message: err.message,
      });
    });

  await db.postgres
    .query(
      `
      INSERT INTO mqtt_sensors (id, "content", type)
      VALUES (${sensor_details.id}, '${JSON.stringify(
        sensor_details.content
      )}', '${sensor_details.type}')
    `,
      {
        raw: true,
        type: QueryTypes.INSERT,
      }
    )
    .then(async (result) => {
      await db.postgres
        .query(
          `SELECT content ->> 'serial_id' as serial_id, type, id  from mqtt_sensors`,
          {
            raw: true,
            type: QueryTypes.SELECT,
          }
        )
        .then(async (result2) => {
          await db.postgres
            .query(
              `
              select content ->> 'serial_id' as serial_id, id
              from sensors
              where content ->> 'serial_id' not in (select content ->> 'serial_id' from mqtt_sensors)
            `,
              {
                raw: true,
                type: QueryTypes.SELECT,
              }
            )
            .then((result3) => {
              update_global_mqtt_sensors_list();
              res.status(200).json({
                sensors: result3,
                newList: result2,
                message: "Success",
              });
            });
        });
    })
    .catch((err) => {
      console.log(err);
      res.status(500).json({
        message: err.message,
      });
    });
});


module.exports = router;
