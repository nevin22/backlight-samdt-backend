var express = require("express");
var router = express.Router();
const db = require("../databricks");

router.get("/:page", async (req, res) => {
  const filters = {
    serial_id: req.query.serial_id || undefined,
    oid: req.query.oid || undefined,
    EDTdate: req.query.EDTdate || undefined,
    playback: req.query.playback || undefined
  };
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

  if (req.params.page > 1) {
    has_recent = true
  }

  try {
    let data = await db.execute_query(`
        SELECT
            sq.attributes,
            sq.timestamp,
            sq.serial_id,
            sq.bbox,  
            sq.delta_id,
            sq.image_url,
            sq.small_circle_id,
            sq.message,
            sq.date_created,
            sq.final_bbox
        FROM bronze.vianalite_snapper_results_v2_0 sq
        WHERE image_url is not null
        ${
            req.query.lastItemCreatedAt
            ? `and timestamp < '${req.query.lastItemCreatedAt}'`
            : ""
        }
        ORDER BY date_created desc
        LIMIT 300
    `);
    // OFFSET ${offset}
    res.status(200).json({
      detections: data,
      message: "Success",
    });
  } catch (error) {
    console.log("errorrr ", error);
    res.status(500).json({
      message: error.message,
    });
  }
});

module.exports = router;
