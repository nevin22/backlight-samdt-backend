var express = require("express");
var router = express.Router();
var moment = require('moment');
const snowflake_client = require('../snowflake');

router.get("/samdt_list", async (req, res) => {
    snowflake_client.execute_query(`
        SELECT
            JOURNEY_ID,
            ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'JOURNEY_ID', JOURNEY_ID,
                'ENTER_TIMESTAMP', ENTER_TIMESTAMP,
                'EXIT_TIMESTAMP', EXIT_TIMESTAMP,
                'IMAGE_URL', CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}'),
                'IS_VALIDATED', IS_VALIDATED,
                'SCENE_NAME', SCENE_NAME,
                'SMALL_CIRCLE_ID', SMALL_CIRCLE_ID,
                'BBOX', BBOX,
                'FOR_PUBLISH', FOR_PUBLISH,
                'VALIDATED_JOURNEY', COALESCE(VALIDATED_JOURNEY, ''),
                'ORDER_INDEX', ORDER_INDEX
            )
            ) AS data
        FROM backlight_samdt
        WHERE
            (ENTER_TIMESTAMP BETWEEN '${req.query.startTime}' AND '${req.query.endTime}')
            AND (
                JOURNEY_ID IS NOT NULL
                OR 
                (JOURNEY_ID IS NULL AND VALIDATED_JOURNEY IS NOT NULL)
            )
        GROUP BY JOURNEY_ID
        ORDER BY MAX(CASE WHEN ORDER_INDEX = 0 THEN ENTER_TIMESTAMP END) DESC
    `, (err, rows) => {
        if (err) {
            console.log('error executing snowflake query -', err);
        }
        let validatedFlattenedDataList = rows.reduce((accumulator, journey) => {
            const validatedEntries = journey.DATA.filter(entry => entry.IS_VALIDATED);
            return accumulator.concat(validatedEntries);
        }, []);

        let filteredRows = rows.filter(d => d.JOURNEY_ID !== null); // removed ang mga walay journey_id kay maguba ang list (igo ra sya gyd gi gamit pra ma add sa validated na data)

        filteredRows.forEach((journey) => {
            // verify if validated
            if (journey.DATA.find(d => d.IS_VALIDATED)) { // check lang any sa data if naay validated
                journey.DATA.forEach((dataObj) => {
                    let validatedData = validatedFlattenedDataList.find((v) => ((v.VALIDATED_JOURNEY === dataObj.JOURNEY_ID) && (v.SCENE_NAME === dataObj.SCENE_NAME)))
                    // console.log('dataObj', dataObj)
                    // console.log('validatedData', validatedData)
                    dataObj.VALIDATED_IMAGE_URL = validatedData.IMAGE_URL;
                    dataObj.VALIDATED_ENTER_TIMESTAMP = validatedData.ENTER_TIMESTAMP;
                    dataObj.VALIDATED_EXIT_TIMESTAMP = validatedData.EXIT_TIMESTAMP;
                })
            }
        });
            
        console.log('filteredRows', filteredRows)
        res.status(200).json({
            detections: filteredRows,
            message: "Success",
        });
    })
})

router.post("/validate_data", async (req, res) => {
    let payload = req.body.body;
    if (payload.isValidated === true) { // meaning gina revalidate
        // first remove tong iyang gipang validate pra ma lisdan
        console.log('111')
        snowflake_client.execute_query(`
            UPDATE backlight_samdt
            SET VALIDATED_JOURNEY = null, IS_VALIDATED = false
            WHERE VALIDATED_JOURNEY = '${payload.selected_data.JOURNEY_ID}'
        `, (err, result) => {
            // update daun
            snowflake_client.execute_query(`
                UPDATE backlight_samdt
                SET VALIDATED_JOURNEY = '${payload.selected_data.JOURNEY_ID}', IS_VALIDATED = true
                WHERE small_circle_id in ('${payload.small_circle_ids.puw}', '${payload.small_circle_ids.ylane}', '${payload.small_circle_ids.orderpoint}', '${payload.small_circle_ids.entrance}');
            `, (err, result) => {
                if (err) {
                    console.log('error executing snowflake query -', err);
                }
                snowflake_client.execute_query(`
                    SELECT
                        JOURNEY_ID,
                        ENTER_TIMESTAMP,
                        EXIT_TIMESTAMP,
                        CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
                        ENTER_TIMESTAMP as VALIDATED_ENTER_TIMESTAMP,
                        EXIT_TIMESTAMP as EXIT_ENTER_TIMESTAMP,
                        CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as VALIDATED_IMAGE_URL,
                        IS_VALIDATED,
                        SCENE_NAME,
                        SMALL_CIRCLE_ID
                    FROM backlight_samdt
                    WHERE small_circle_id in ('${payload.small_circle_ids.puw}', '${payload.small_circle_ids.ylane}', '${payload.small_circle_ids.orderpoint}', '${payload.small_circle_ids.entrance}');
                `, (err, result2) => {
                    res.status(200).json({
                        updatedData: result2,
                        message: "Success",
                    });
                })
            })
        })
    } else {
        snowflake_client.execute_query(`
            UPDATE backlight_samdt
            SET VALIDATED_JOURNEY = '${payload.selected_data.JOURNEY_ID}', IS_VALIDATED = true
            WHERE small_circle_id in ('${payload.small_circle_ids.puw}', '${payload.small_circle_ids.ylane}', '${payload.small_circle_ids.orderpoint}', '${payload.small_circle_ids.entrance}');
        `, (err, result) => {
            if (err) {
                console.log('error executing snowflake query -', err);
            }
            console.log('res', result)
            snowflake_client.execute_query(`
                SELECT
                    JOURNEY_ID,
                    ENTER_TIMESTAMP,
                    EXIT_TIMESTAMP,
                    CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
                    ENTER_TIMESTAMP as VALIDATED_ENTER_TIMESTAMP,
                    EXIT_TIMESTAMP as EXIT_ENTER_TIMESTAMP,
                    CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as VALIDATED_IMAGE_URL,
                    IS_VALIDATED,
                    SCENE_NAME,
                    SMALL_CIRCLE_ID
                FROM backlight_samdt
                WHERE small_circle_id in ('${payload.small_circle_ids.puw}', '${payload.small_circle_ids.ylane}', '${payload.small_circle_ids.orderpoint}', '${payload.small_circle_ids.entrance}');
            `, (err, result2) => {
                res.status(200).json({
                    updatedData: result2,
                    message: "Success",
                });
            })
        })
    }
})

router.get("/samdt_edit_list", async (req, res) => {
    let puw = JSON.parse(req.query.data.find(data => JSON.parse(data).SCENE_NAME === 'Scene Pull Up Window'));
    let ylane = JSON.parse(req.query.data.find(data => JSON.parse(data).SCENE_NAME === 'Scene Y Lane Merge'));
    let orderpoint = JSON.parse(req.query.data.find(data => JSON.parse(data).SCENE_NAME === 'Scene Order Point Outside Lane'));
    let entrance = JSON.parse(req.query.data.find(data => JSON.parse(data).SCENE_NAME === 'Scene Entrance Outside Lane'));

    snowflake_client.execute_query(`
    (
        SELECT
            JOURNEY_ID,
            ENTER_TIMESTAMP,
            EXIT_TIMESTAMP,
            CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
            IS_VALIDATED,
            SCENE_NAME,
            SMALL_CIRCLE_ID,
            COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
        FROM backlight_samdt
        WHERE IS_VALIDATED <> TRUE AND ENTER_TIMESTAMP > '${puw.ENTER_TIMESTAMP}' AND SCENE_NAME = '${puw.SCENE_NAME}'
        order by ENTER_TIMESTAMP ASC
        LIMIT 10
    )
    UNION ALL
    (
        SELECT
            JOURNEY_ID,
            ENTER_TIMESTAMP,
            EXIT_TIMESTAMP,
            CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
            IS_VALIDATED,
            SCENE_NAME,
            SMALL_CIRCLE_ID,
            COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
        FROM backlight_samdt
        WHERE IS_VALIDATED <> TRUE AND ENTER_TIMESTAMP < '${puw.ENTER_TIMESTAMP}' AND SCENE_NAME = '${puw.SCENE_NAME}'
        order by ENTER_TIMESTAMP DESC
        LIMIT 10
    )
    UNION ALL
    (
        SELECT
            JOURNEY_ID,
            ENTER_TIMESTAMP,
            EXIT_TIMESTAMP,
            CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
            IS_VALIDATED,
            SCENE_NAME,
            SMALL_CIRCLE_ID,
            COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
        FROM backlight_samdt
        WHERE IS_VALIDATED <> TRUE AND ENTER_TIMESTAMP > '${ylane.ENTER_TIMESTAMP}' AND SCENE_NAME = '${ylane.SCENE_NAME}'
        ORDER BY ENTER_TIMESTAMP ASC
        LIMIT 10
    )
    UNION ALL
    (
        SELECT
            JOURNEY_ID,
            ENTER_TIMESTAMP,
            EXIT_TIMESTAMP,
            CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
            IS_VALIDATED,
            SCENE_NAME,
            SMALL_CIRCLE_ID,
            COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
        FROM backlight_samdt
        WHERE IS_VALIDATED <> TRUE AND ENTER_TIMESTAMP < '${ylane.ENTER_TIMESTAMP}' AND SCENE_NAME = '${ylane.SCENE_NAME}'
        ORDER BY ENTER_TIMESTAMP DESC
        LIMIT 10
    )
    UNION ALL
    (
        SELECT
            JOURNEY_ID,
            ENTER_TIMESTAMP,
            EXIT_TIMESTAMP,
            CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
            IS_VALIDATED,
            SCENE_NAME,
            SMALL_CIRCLE_ID,
            COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
        FROM backlight_samdt
        WHERE IS_VALIDATED <> TRUE AND ENTER_TIMESTAMP > '${orderpoint.ENTER_TIMESTAMP}' AND SCENE_NAME = '${orderpoint.SCENE_NAME}'
        ORDER BY ENTER_TIMESTAMP ASC
        LIMIT 10
    )
    UNION ALL
    (
        SELECT
            JOURNEY_ID,
            ENTER_TIMESTAMP,
            EXIT_TIMESTAMP,
            CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
            IS_VALIDATED,
            SCENE_NAME,
            SMALL_CIRCLE_ID,
            COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
        FROM backlight_samdt
        WHERE IS_VALIDATED <> TRUE AND ENTER_TIMESTAMP < '${orderpoint.ENTER_TIMESTAMP}' AND SCENE_NAME = '${orderpoint.SCENE_NAME}'
        ORDER BY ENTER_TIMESTAMP DESC
        LIMIT 10
    )
    UNION ALL
    (
        SELECT
            JOURNEY_ID,
            ENTER_TIMESTAMP,
            EXIT_TIMESTAMP,
            CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
            IS_VALIDATED,
            SCENE_NAME,
            SMALL_CIRCLE_ID,
            COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
        FROM backlight_samdt
        WHERE IS_VALIDATED <> TRUE AND ENTER_TIMESTAMP > '${entrance.ENTER_TIMESTAMP}' AND SCENE_NAME = '${entrance.SCENE_NAME}'
        ORDER BY ENTER_TIMESTAMP ASC
        LIMIT 10
    )
    UNION ALL
    (
        SELECT
            JOURNEY_ID,
            ENTER_TIMESTAMP,
            EXIT_TIMESTAMP,
            CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
            IS_VALIDATED,
            SCENE_NAME,
            SMALL_CIRCLE_ID,
            COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
        FROM backlight_samdt
        WHERE IS_VALIDATED <> TRUE AND ENTER_TIMESTAMP < '${entrance.ENTER_TIMESTAMP}' AND SCENE_NAME = '${entrance.SCENE_NAME}'
        ORDER BY ENTER_TIMESTAMP DESC
        LIMIT 10
    )
    `, (err, rows) => {
        if (err) {
            console.log('error executing snowflake query -', err);
            res.status(500).json({
                message: "Error",
            });
        }

        let items = [...rows];
        // Group journeys by SCENE_NAME
        const groupedItems = items.reduce((acc, journey) => {
            if (!acc[journey.SCENE_NAME]) {
                acc[journey.SCENE_NAME] = [];
            }
            acc[journey.SCENE_NAME].push(journey);
            return acc;
        }, {});

        // Sort each group by ENTER_TIMESTAMP
        for (const scene in groupedItems) {
            groupedItems[scene].sort((a, b) => {
                return new Date(a.ENTER_TIMESTAMP) - new Date(b.ENTER_TIMESTAMP);
            });
        }

        console.log('hehe')
        res.status(200).json({
            rows: groupedItems,
            message: "Success",
        });
    })
})

router.post("/sync_data_to_manifest", async (req, res) => {
    snowflake_client.execute_query(`
        UPDATE backlight_samdt
        SET FOR_PUBLISH = true
        WHERE IS_VALIDATED = true;
    `, (err, result) => {
        if (err) {
            console.log('error', err);
            res.status(500).json({
                message: "error updating",
            });
        }
        res.status(200).json({
            message: "Success",
        });
    })
})

module.exports = router;

// const db = require("../databricks");
// var SAS_TOKEN = process.env.SAS_TOKEN;

// router.get("/:page", async (req, res) => {
//     const filters = {
//         serial_id: req.query.serial_id || undefined,
//         date: req.query.date || undefined,
//         network: req.query.network || undefined,
//         serviceApplet: req.query.serviceApplet || undefined,
//         resultType: req.query.resultType || undefined,
//     };

//     const sortType = req.query.sortType === '2' ? 'date_created' : 'timestamp';

//     let debug_mode = req.query.serial_id === 'debug_mode';

//     let filters_query = [];
//     Object.keys(filters).forEach(d => {
//         if (filters[d] && filters[d] !== undefined) {
//             if (d === 'serial_id') {
//                 filters_query.push(`and sq.serial_id = '${filters[d]}'`)
//             } else if (d === 'date') {
//                 filters_query.push(`and sq.stage_timestamp < '${filters[d]}'`)
//             } else if (d === 'network') {
//                 filters_query.push(`and network_id = ${filters[d]}`)
//             } else if (d === 'serviceApplet') {
//                 filters_query.push(`and sa_type = '${filters[d].toUpperCase()}'`)
//             } else if (d === 'resultType') {
//                 if (filters[d] === '1') {
//                     filters_query.push(`and message = 'Success'`)
//                 } else if (filters[d] === '2') {
//                     filters_query.push(`and message in ('No face Detected', 'No Face Detected')`)
//                 }
//             }
//         }
//     })
//     try {
//         let data = await db.execute_query(`
//         SELECT
//             sq.attributes,
//             sq.timestamp,
//             sq.serial_id,
//             sq.bbox,
//             sq.delta_id,
//             concat(sq.image_url, '${SAS_TOKEN}') as image_url,
//             sq.small_circle_id,
//             sq.message,
//             sq.date_created,
//             sq.final_bbox,
//             sq.stage_timestamp,
//             sq.sa_type,
//             sq.network_id,
//             sq.bbox_used,
//             sq.all_bbox
//         FROM bronze.vianalite_snapper_results_v2_0 sq
//         WHERE image_url is not null
//         ${filters_query.join(" ")}
//         ${req.query.lastItemCreatedAt
//                 ? `and ${sortType} < '${req.query.lastItemCreatedAt}'`
//                 : ""
//             }
//         ORDER BY ${sortType} desc
//         LIMIT 100
//     `);
//         res.status(200).json({
//             detections: data,
//             message: "Success",
//         });
//     } catch (error) {
//         console.log("errorrr ", error);
//         res.status(500).json({
//             message: error.message,
//         });
//     }
// });
