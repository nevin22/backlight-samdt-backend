var express = require("express");
var router = express.Router();
const db = require("../databricks");

// router.get("/test", async(req, res) => {
//     let data = await db.execute_query(`
//         select * from bronze.vianalite_snapper_results_v2_0
//         limit 10
//     `);

//     // console.table(data)

//     res.status(200).json({
//         data,
//         message: "Success",
//     });
// })

router.get("/:page", async (req, res) => {
  const offset = (req.params.page - 1) * 100;
  let has_recent = false;

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
