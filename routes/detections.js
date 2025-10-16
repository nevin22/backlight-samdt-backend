var express = require("express");
var router = express.Router();
var moment = require('moment');
const snowflake_client = require('../snowflake');
const { Storage } = require('@google-cloud/storage');

const storage = new Storage({
    keyFilename: process.env.GCP_STORAGE_KEY
});

async function generateSignedUrl(fileName, subtype) {
    const bucketName = subtype === 'axis' ? process.env.GCP_BUCKET_DEV_AXIS : process.env.GCP_BUCKET_DEV;
    const options = {
        version: 'v4',
        action: 'read',
        expires: Date.now() + 15 * 60 * 1000, // 15 minutes
    };
    const [url] = await storage
        .bucket(bucketName)
        .file(fileName)
        .getSignedUrl(options);

    return url;
}

function getObjectPath(gcsUrl) {
    const match = gcsUrl.match(/https?:\/\/storage\.googleapis\.com\/[^\/]+\/(.+)/);
    if (!match) throw new Error('Invalid GCS URL format');
    return decodeURIComponent(match[1]);
}


router.get("/samdt_list", async (req, res) => {
    let filters = (req.query.filters && JSON.parse(req.query.filters)) || undefined;
    if (filters === undefined) {
        res.status(500).json({
            message: "No filter"
        });
    } else {
        snowflake_client.execute_query(`
        SELECT
            JOURNEY_ID,
            ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'BA_TYPE', BA_TYPE,
                'ATTRIBUTES', ATTRIBUTES,
                'JOURNEY_ID', JOURNEY_ID,
                'ENTER_TIMESTAMP', ENTER_TIMESTAMP,
                'EXIT_TIMESTAMP', EXIT_TIMESTAMP,
                'IMAGE_URL', COALESCE(gnc_result, IMAGE_URL),
                'IS_VALIDATED', IS_VALIDATED,
                'SCENE_NAME', SCENE_NAME,
                'SMALL_CIRCLE_ID', SMALL_CIRCLE_ID,
                'BBOX', BBOX,
                'FINAL_BBOX', COALESCE(FINAL_BBOX, ''),
                'FOR_PUBLISH', FOR_PUBLISH,
                'VALIDATED_JOURNEY', COALESCE(VALIDATED_JOURNEY, ''),
                'ORDER_INDEX', ORDER_INDEX,
                'IS_BA', IS_BA,
                'SUBTYPE', SUBTYPE
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
            AND NETWORK_NAME = '${filters.network}'
            AND SITE_NAME = '${filters.site}'
        GROUP BY JOURNEY_ID
        ORDER BY MAX(ENTER_TIMESTAMP) DESC
    `, async (err, rows) => {
            if (err) {
                console.log('error executing snowflake query -', err);
                res.status(500).json({
                    message: "Internal server error"
                });
            }
            let validatedFlattenedDataList = rows.reduce((accumulator, journey) => {
                const validatedEntries = journey.DATA.filter(entry => entry.IS_VALIDATED);
                return accumulator.concat(validatedEntries);
            }, []);


            // let test = await generateSignedUrl(getObjectPath('https://storage.googleapis.com/bkt-viana-dev-vianagncstoragedev/samdt/2025-08-04/Q2UV-FREJ-RTQY-42db21db.png'))

            let filteredRows = rows.filter(d => d.JOURNEY_ID !== null); // removed ang mga walay journey_id kay maguba ang list (igo ra sya gyd gi gamit pra ma add sa validated na data)
            let table_columns = tableColumns(filteredRows);
            let tableColumnNames = table_columns.map(d => d.sceneName);
            filteredRows.forEach(data => { // if kulang ang row ug isa ka item.. butngan natog skeleton lang 
                if (data.DATA.length !== tableColumnNames.length) {
                    let dataScenes = data.DATA.map(d => d.SCENE_NAME);
                    const scenesNotInTableColumns = tableColumnNames.filter(item => !dataScenes.includes(item));

                    scenesNotInTableColumns.forEach(d => {
                        data.DATA.push({
                            ...data.DATA[0],
                            SMALL_CIRCLE_ID: undefined,
                            IMAGE_URL: undefined,
                            ENTER_TIMESTAMP: undefined,
                            EXIT_TIMESTAMP: undefined,
                            VALIDATED_JOURNEY: undefined,
                            SCENE_NAME: d,
                            ORDER_INDEX: table_columns.find(d2 => d2.sceneName === d).index,
                            NO_DATA: true
                        })
                    })
                }
            })

            validatedFlattenedDataList.forEach((validatedData) => {
                let toUpdateIndex = filteredRows.findIndex(d => d.JOURNEY_ID === validatedData.VALIDATED_JOURNEY);
                let dataIndex = filteredRows[toUpdateIndex].DATA.findIndex(d => ((d.JOURNEY_ID === validatedData.VALIDATED_JOURNEY) && (d.SCENE_NAME === validatedData.SCENE_NAME)));
                filteredRows[toUpdateIndex].DATA[dataIndex] = {
                    ...filteredRows[toUpdateIndex].DATA[dataIndex],
                    VALIDATED_IMAGE_URL: validatedData.IMAGE_URL,
                    VALIDATED_ENTER_TIMESTAMP: validatedData.ENTER_TIMESTAMP,
                    VALIDATED_EXIT_TIMESTAMP: validatedData.EXIT_TIMESTAMP,
                    IS_VALIDATED: true, // this is for frontend purpose only
                    VALIDATED_JOURNEY: validatedData.VALIDATED_JOURNEY, // this is for frontend purpose only
                    IS_VALIDATED_FULL_JOURNEY: true, // this is for frontend purpose only
                    IS_FOR_PUBLISH_FULL_JOURNEY: validatedData.FOR_PUBLISH, // this is for frontend purpose only
                    NO_DATA: false,
                    BA_TYPE: validatedData.BA_TYPE,
                    SUBTYPE: validatedData.SUBTYPE
                }
            })
            // sign image urls
            filteredRows = await Promise.all(
                filteredRows.map(async (row) => ({
                    ...row,
                    DATA: await Promise.all(
                        row.DATA.map(async (item) => {
                            if (!item.IMAGE_URL) return item;
                            try {
                                const image_url_signedUrl = await generateSignedUrl(getObjectPath(item.IMAGE_URL), item.SUBTYPE);

                                let validated_image_url_signed = item.VALIDATED_IMAGE_URL;

                                if (validated_image_url_signed) {
                                    validated_image_url_signed = await generateSignedUrl(getObjectPath(item.VALIDATED_IMAGE_URL), item.SUBTYPE)
                                }

                                return { ...item, IMAGE_URL: image_url_signedUrl, VALIDATED_IMAGE_URL: validated_image_url_signed };
                            } catch (e) {
                                console.error("Failed to sign URL:", item.IMAGE_URL, e);
                                return item; // keep original if signing fails
                            }
                        })
                    ),
                }))
            );
            res.status(200).json({
                detections: filteredRows,
                message: "Success",
            });
        })
    }

})

let tableColumns = (filteredRows) => {
    const sceneNamesByIndex = {};
    filteredRows.forEach(journey => {
        journey.DATA.forEach(entry => {
            const index = parseInt(entry.ORDER_INDEX);
            const sceneName = entry.SCENE_NAME;

            if (!sceneNamesByIndex[index]) {
                sceneNamesByIndex[index] = sceneName;
            }
        });
    });

    const result = Object.keys(sceneNamesByIndex).map(index => {
        return { index: parseInt(index), sceneName: sceneNamesByIndex[index] };
    });

    return result.sort((a, b) => b.index - a.index);
}


router.post("/validate_data", async (req, res) => {
    let payload = req.body.body;
    let isBalk = payload.isBalk;
    let eventType = payload.eventType || 'Warm Exit';

    let fov_names = Object.keys(payload.small_circle_ids)
    let sm_ids = fov_names.map(d => payload.small_circle_ids[`${d}`])

    // verify if wala pay validated na data within sa selected na mga small circle ids
    snowflake_client.execute_query(`
        select * from backlight_samdt where small_circle_id in (${sm_ids.map(value => `'${value}'`).join(', ')});
    `, (err, result) => {
        if (err) {
            console.log('error executing snowflake query -', err);
        }
        let validated_items = result.filter(r => r.IS_VALIDATED)
        if (validated_items.length) { // if naa gyud then return ang error with message pra kabalo ang user
            res.status(500).json({
                message: `Validation Error!\n\nThe selected values include data that has already been validated.\n[ ${validated_items.map(d => d.SCENE_NAME).join(", ")} ]\n\nPlease select a new value for the scenes listed above.
                `,
            });
        } else {
            snowflake_client.execute_query(`
                UPDATE backlight_samdt
                SET VALIDATED_JOURNEY = '${payload.selected_data.JOURNEY_ID}', IS_VALIDATED = true, BA_TYPE = '${eventType}'
                WHERE small_circle_id in (${sm_ids.map(value => `'${value}'`).join(', ')})
            `, (err, result) => {
                if (err) {
                    console.log('error executing snowflake query -', err);
                }
                snowflake_client.execute_query(`
                    SELECT
                        JOURNEY_ID,
                        BA_TYPE,
                        ENTER_TIMESTAMP,
                        EXIT_TIMESTAMP,
                        COALESCE(GNC_RESULT, IMAGE_URL) AS IMAGE_URL,
                        ENTER_TIMESTAMP AS VALIDATED_ENTER_TIMESTAMP,
                        EXIT_TIMESTAMP AS EXIT_ENTER_TIMESTAMP,
                        COALESCE(GNC_RESULT, IMAGE_URL) AS VALIDATED_IMAGE_URL,
                        IS_VALIDATED,
                        SCENE_NAME,
                        SMALL_CIRCLE_ID,
                        ORDER_INDEX,
                        VALIDATED_JOURNEY,
                        SUBTYPE,
                        IS_BA,
                        TRUE AS IS_VALIDATED_FULL_JOURNEY
                    FROM backlight_samdt
                    WHERE small_circle_id IN (${sm_ids.map(value => `'${value}'`).join(', ')});
                `, async (err, result2) => {

                    let mutable_result = result2 ? [...result2] : [];


                    // sign image url
                    mutable_result = await Promise.all(
                        mutable_result.map(async (row) => {
                            if (row.IMAGE_URL) {
                                const signedUrl = await generateSignedUrl(getObjectPath(row.IMAGE_URL), row.SUBTYPE);
                                return {
                                    ...row,
                                    IMAGE_URL: signedUrl,
                                    VALIDATED_IMAGE_URL: signedUrl,
                                };
                            }
                            return row;
                        })
                    );
                    res.status(200).json({
                        updatedData: mutable_result,
                        message: "Success",
                    });
                })
            })
        }
    })
})

router.get("/samdt_edit_list", async (req, res) => {
    let parsedData = req.query.data.map(d => JSON.parse(d))
    let parseDataScenes = parsedData.map(d => d.SCENE_NAME);

    if (parseDataScenes.length !== req.query.tableColumns) {
        let sourceData = parsedData[0];
        req.query.tableColumns.forEach((col) => {
            let colD = JSON.parse(col);
            if (!parseDataScenes.includes(colD.sceneName)) { // manually add data when specfic scene does not exist
                parsedData.push({
                    ...sourceData,
                    SCENE_NAME: colD.sceneName
                })
            }
        })
    }

    let generatedQuery = parsedData.map((journey_item, index) => {
        return `
            ${index === 0 ? '' : 'UNION ALL'}
            (
                SELECT
                    JOURNEY_ID,
                    ATTRIBUTES,
                    ENTER_TIMESTAMP,
                    EXIT_TIMESTAMP,
                    COALESCE(GNC_RESULT, IMAGE_URL) AS IMAGE_URL,
                    IS_VALIDATED,
                    SCENE_NAME,
                    SMALL_CIRCLE_ID,
                    ORDER_INDEX,
                    BBOX,
                    FINAL_BBOX,
                    SUBTYPE,
                    COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
                FROM backlight_samdt
                WHERE
                    IS_VALIDATED <> TRUE AND
                    DATE(ENTER_TIMESTAMP) = DATE('${journey_item.ENTER_TIMESTAMP || journey_item.VALIDATED_ENTER_TIMESTAMP}') AND
                    ENTER_TIMESTAMP > '${journey_item.ENTER_TIMESTAMP || journey_item.VALIDATED_ENTER_TIMESTAMP}' AND
                    SCENE_NAME = '${journey_item.SCENE_NAME}' AND
                    IMAGE_URL IS NOT NULL AND
                    IMAGE_URL != ''
                order by ENTER_TIMESTAMP ASC
                LIMIT 20
            )
            UNION ALL
            (
                SELECT
                    JOURNEY_ID,
                    ATTRIBUTES,
                    ENTER_TIMESTAMP,
                    EXIT_TIMESTAMP,
                    COALESCE(GNC_RESULT, IMAGE_URL) AS IMAGE_URL,
                    IS_VALIDATED,
                    SCENE_NAME,
                    SMALL_CIRCLE_ID,
                    ORDER_INDEX,
                    BBOX,
                    FINAL_BBOX,
                    SUBTYPE,
                    COALESCE(VALIDATED_JOURNEY, '') AS VALIDATED_JOURNEY
                FROM backlight_samdt
                WHERE
                    IS_VALIDATED <> TRUE AND
                    DATE(ENTER_TIMESTAMP) = DATE('${journey_item.ENTER_TIMESTAMP || journey_item.VALIDATED_ENTER_TIMESTAMP}') AND
                    ENTER_TIMESTAMP < '${journey_item.ENTER_TIMESTAMP || journey_item.VALIDATED_ENTER_TIMESTAMP}' AND
                    SCENE_NAME = '${journey_item.SCENE_NAME}' AND
                    IMAGE_URL IS NOT NULL AND
                    IMAGE_URL != ''
                order by ENTER_TIMESTAMP DESC
                LIMIT 20
            )
        `
    }).join('');

    snowflake_client.execute_query(generatedQuery, async (err, rows) => {
        if (err) {
            console.log('error executing snowflake query -', err);
            res.status(500).json({
                message: "Error",
            });
        }

        let items = [...rows];
        // sign image URL
        items = await Promise.all(
            items.map(async (row) => {
                if (row.IMAGE_URL) {
                    const signedUrl = await generateSignedUrl(getObjectPath(row.IMAGE_URL), row.SUBTYPE);
                    return {
                        ...row,
                        IMAGE_URL: signedUrl
                    };
                }
                return row;
            })
        );

        // Group journeys by SCENE_NAME
        let groupedItems = items.reduce((acc, journey) => {
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

        res.status(200).json({
            rows: groupedItems,
            message: "Success",
        });
    })
})

router.post("/sync_data_to_manifest", async (req, res) => {
    let payload = req.body.body;
    snowflake_client.execute_query(`
        UPDATE backlight_samdt
        SET FOR_PUBLISH = true
        WHERE IS_VALIDATED = true
        AND NETWORK_NAME = '${payload.network}'
        AND SITE_NAME = '${payload.site}'
        ${payload.startTime ? ` AND (ENTER_TIMESTAMP BETWEEN '${payload.startTime}' AND '${payload.endTime}')` : ''}
    `, (err, result) => {
        if (err) {
            console.log('error', err);
            res.status(500).json({
                message: "error updating",
            });
        } else {
            res.status(200).json({
                message: "Success",
            });
        }
    })
})

router.get("/get_network_options", async (req, res) => {
    snowflake_client.execute_query(`
        SELECT DISTINCT ARRAY_AGG(DISTINCT network_name) AS networks
        FROM backlight_samdt;
    `, (err, result) => {
        if (err) {
            res.status(500).json({
                message: "error fetching",
            });
        }

        res.status(200).json({
            message: "Success",
            data: result[0]
        });
    })
})

router.get("/get_site_options", async (req, res) => {
    let network = req.query.network;
    snowflake_client.execute_query(`
        SELECT DISTINCT ARRAY_AGG(DISTINCT site_name) AS sites
        FROM backlight_samdt
        WHERE network_name = '${network}'
    `, (err, result) => {
        if (err) {
            res.status(500).json({
                message: "error fetching",
            });
        }
        res.status(200).json({
            message: "Success",
            data: result[0]
        });
    })
})

router.post("/invalidate_data", async (req, res) => {
    let payload = req.body.body;
    snowflake_client.execute_query(`
        UPDATE backlight_samdt
        SET VALIDATED_JOURNEY = null, IS_VALIDATED = false, BA_TYPE = null
        WHERE VALIDATED_JOURNEY = '${payload.journey_id}'
    `, (err, result) => {
        if (err) {
            res.status(500).json({
                message: "error updating",
            });
        }

        snowflake_client.execute_query(`
            SELECT
                JOURNEY_ID,
                BA_TYPE,
                ENTER_TIMESTAMP,
                EXIT_TIMESTAMP,
                CONCAT(
                    COALESCE(GNC_RESULT, IMAGE_URL),
                    CASE 
                        WHEN GNC_RESULT IS NOT NULL THEN '${process.env.SAMDT_SAS_TOKEN}'
                        ELSE '${process.env.SAMDT_SAS_TOKEN_AXIS}'
                    END
                ) AS IMAGE_URL,
                ENTER_TIMESTAMP as VALIDATED_ENTER_TIMESTAMP,
                EXIT_TIMESTAMP as EXIT_ENTER_TIMESTAMP,
                CONCAT(
                    COALESCE(GNC_RESULT, IMAGE_URL),
                    CASE 
                        WHEN GNC_RESULT IS NOT NULL THEN '${process.env.SAMDT_SAS_TOKEN}'
                        ELSE '${process.env.SAMDT_SAS_TOKEN_AXIS}'
                    END
                ) AS VALIDATED_IMAGE_URL,
                IS_VALIDATED,
                SCENE_NAME,
                SMALL_CIRCLE_ID,
                ORDER_INDEX,
                VALIDATED_JOURNEY,
                IS_BA,
                BBOX,
                FINAL_BBOX,
                false as IS_VALIDATED_FULL_JOURNEY
            FROM backlight_samdt
            WHERE JOURNEY_ID = '${payload.journey_id}'
        `, (err, result2) => {
            res.status(200).json({
                updatedData: result2,
                message: "Success",
            });
        })

    })
})

module.exports = router;