var express = require("express");
var router = express.Router();
const db = require('../database/viana');
const { QueryTypes } = require('sequelize');

router.get("/detections/:page", async (req, res) => {
  const offset = (req.params.page - 1) * 100;

  await db.postgres
    .query(
      `
    SELECT
        sq.lpr_result,
        sq.timestamp_str,
        sq.api_key,
        sq.serial_id,
        sq.track_object,  
        sq.id,
        sq.image_url,
        "createdAt"
    FROM mqtt_detections sq
    WHERE snapshot_generated = true and image_url is not null and type ->> 'name' = '${req.query.type}'
    ${
      req.query.lastItemCreatedAt
        ? `and "timestamp_str" < '${req.query.lastItemCreatedAt}'`
        : ""
    }
    ORDER BY "timestamp_str" desc
    LIMIT 100 OFFSET ${offset}
    `,
      {
        raw: true,
        type: QueryTypes.SELECT,
      }
    )
    .then(async (result) => {
      res.status(200).json({
        detections: result,
        message: "Success",
      });
    })
    .catch((err) => {
      console.log("filter err", err);
      res.status(500).json({
        message: err.message,
      });
    });
});

router.get("/detections_filter/:page", async (req, res) => {
  const filters = {
    serial_id: req.query.serial_id || undefined,
    oid: req.query.oid || undefined,
    EDTdate: req.query.EDTdate || undefined,
    playback: req.query.playback || undefined
  };
  const offset = (req.params.page - 1) * 100;
  let debug_mode = req.query.serial_id === 'debug_mode';

  let filters_query = [];
  Object.keys(filters).forEach(d => {
    if (filters[d] && filters[d] !== undefined) {
      if (d === 'serial_id') {
        if (debug_mode) {
          filters_query = [`and (track_object ->> 'is_test') = 'true'`]
        } else {
          filters_query.push(`and ((track_object ->> 'is_test') = 'false' OR (track_object ->> 'is_test') is null)`);
          filters_query.push(`and sq.serial_id = '${filters[d]}'`)
        }  
      } else if (d === 'EDTdate') {
        filters_query.push(`and sq.timestamp_date < '${filters[d]}'`)
      } else if (d === 'oid') {
        filters_query.push(`and sq.oid = '${filters[d]}'`)
      }
    }
  })
  
  await db.postgres
    .query(
      `
      SELECT
        sq.lpr_result,
        sq.timestamp_str,
        sq.api_key,
        sq.serial_id,
        sq.track_object,  
        sq.id,
        sq.image_url,
        "createdAt"
      FROM mqtt_detections sq
      WHERE snapshot_generated = true and image_url is not null and type ->> 'name' =  '${req.query.type}'
        ${filters_query.join(" ")}
        ${
          req.query.lastItemCreatedAt
            ? `and timestamp_str < ${req.query.lastItemCreatedAt}`
            : ""
        }
        ORDER BY timestamp_str desc
        ${""}
      LIMIT 100 OFFSET ${offset}
      `,
      {
        raw: true,
        type: QueryTypes.SELECT,
      }
    )
    .then(async (result) => {
      res.status(200).json({
        detections: result,
        message: "Success",
      });
    })
    .catch((err) => {
      console.log("filter err", err);
      res.status(500).json({
        message: err.message,
      });
    });
});

module.exports = router;