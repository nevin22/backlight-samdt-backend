var express = require("express");
var router = express.Router();
var moment = require('moment');
const snowflake_client = require('../snowflake');

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
                'IMAGE_URL', CONCAT(
                    COALESCE(gnc_result, IMAGE_URL),
                    CASE 
                        WHEN gnc_result IS NOT NULL THEN '${process.env.SAMDT_SAS_TOKEN}'
                        ELSE '${process.env.SAMDT_SAS_TOKEN_AXIS}'
                    END
                ),
                'IS_VALIDATED', IS_VALIDATED,
                'SCENE_NAME', SCENE_NAME,
                'SMALL_CIRCLE_ID', SMALL_CIRCLE_ID,
                'BBOX', BBOX,
                'FINAL_BBOX', COALESCE(FINAL_BBOX, ''),
                'FOR_PUBLISH', FOR_PUBLISH,
                'VALIDATED_JOURNEY', COALESCE(VALIDATED_JOURNEY, ''),
                'ORDER_INDEX', ORDER_INDEX,
                'IS_BA', IS_BA
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
    `, (err, rows) => {
            if (err) {
                console.log('error executing snowflake query -', err);
                res.status(500).json({
                    message: "Internal server error"
                });
            }
            // filter out katong mga naay validated journey id pero wala nay ga match sa iyang orignal journey_id meaning na pili na sa lahi na session tanan niya data
            // let rows_copy = [...rows]
            // rows_copy = rows.filter(row => {
            //     const allDataAreValidated = row.DATA.every(dataItem => dataItem.VALIDATED_JOURNEY !== (null || ""));
                
            //     // if atleast isa ka data kay naay null na validated_journey .. goods ra sya i return
            //     if (!allDataAreValidated) return true;
                
            //     // Otherwise, check if at least one `VALIDATED_JOURNEY` matches the `JOURNEY_ID`.
            //     return row.DATA.some(dataItem => dataItem.VALIDATED_JOURNEY === row.JOURNEY_ID);
            // });

            // console.log('before -', rows.length, ' -- ', 'after -', rows_copy.length);

            let validatedFlattenedDataList = rows.reduce((accumulator, journey) => {
                const validatedEntries = journey.DATA.filter(entry => entry.IS_VALIDATED);
                return accumulator.concat(validatedEntries);
            }, []);

            let filteredRows = rows.filter(d => d.JOURNEY_ID !== null); // removed ang mga walay journey_id kay maguba ang list (igo ra sya gyd gi gamit pra ma add sa validated na data)

            // filteredRows.forEach((journey) => {
            //     // verify if validated
            //     if (journey.DATA.find(d => d.IS_VALIDATED)) { // check lang any sa data if naay validated
            //         console.log('journey.DATA', journey.DATA)
            //         journey.DATA.forEach((dataObj) => {
            //             let validatedData = validatedFlattenedDataList.find((v) => ((v.VALIDATED_JOURNEY === dataObj.JOURNEY_ID) && (v.SCENE_NAME === dataObj.SCENE_NAME)))
            //             console.log('dataObj', dataObj)
            //             console.log('validatedData', validatedData)
            //             dataObj.VALIDATED_IMAGE_URL = validatedData.IMAGE_URL;
            //             dataObj.VALIDATED_ENTER_TIMESTAMP = validatedData.ENTER_TIMESTAMP;
            //             dataObj.VALIDATED_EXIT_TIMESTAMP = validatedData.EXIT_TIMESTAMP;
            //         })
            //     }
            // });
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
                }
            })

            // remove sessions where all of its values are assigned to another session
            // console.log('filteredRows', filteredRows.find(d => d.JOURNEY_ID = '9873427c-1d9f-4afd-b6e9-54d7a7aedb16'))

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
                        CONCAT(
                            COALESCE(GNC_RESULT, IMAGE_URL),
                            CASE 
                                WHEN GNC_RESULT IS NOT NULL THEN '${process.env.SAMDT_SAS_TOKEN}'
                                ELSE '${process.env.SAMDT_SAS_TOKEN_AXIS}'
                            END
                        ) AS IMAGE_URL,
                        ENTER_TIMESTAMP AS VALIDATED_ENTER_TIMESTAMP,
                        EXIT_TIMESTAMP AS EXIT_ENTER_TIMESTAMP,
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
                        TRUE AS IS_VALIDATED_FULL_JOURNEY
                    FROM backlight_samdt
                    WHERE small_circle_id IN (${sm_ids.map(value => `'${value}'`).join(', ')});
                `, (err, result2) => {
                    res.status(200).json({
                        updatedData: result2,
                        message: "Success",
                    });
                })
            })
        }
    })


    // if (payload.isValidated === true) { // meaning gina revalidate
    //     // first remove tong iyang gipang validate pra ma lisdan
    //     snowflake_client.execute_query(`
    //         UPDATE backlight_samdt
    //         SET VALIDATED_JOURNEY = null, IS_VALIDATED = false, BALK = null
    //         WHERE VALIDATED_JOURNEY = '${payload.selected_data.JOURNEY_ID}'
    //     `, (err, result) => {
    //         // update daun
    //         let fov_names = Object.keys(payload.small_circle_ids)
    //         let sm_ids = fov_names.map(d => payload.small_circle_ids[`${d}`])

    //         snowflake_client.execute_query(`
    //             UPDATE backlight_samdt
    //             SET VALIDATED_JOURNEY = '${payload.selected_data.JOURNEY_ID}', IS_VALIDATED = true, BA_TYPE = '${eventType}'
    //             WHERE small_circle_id in (${sm_ids.map(value => `'${value}'`).join(', ')})
    //         `, (err, result) => {
    //             if (err) {
    //                 console.log('error executing snowflake query -', err);
    //             }
    //             snowflake_client.execute_query(`
    //                 SELECT
    //                     JOURNEY_ID,
    //                     ENTER_TIMESTAMP,
    //                     BA_TYPE,
    //                     EXIT_TIMESTAMP,
    //                     CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
    //                     ENTER_TIMESTAMP as VALIDATED_ENTER_TIMESTAMP,
    //                     EXIT_TIMESTAMP as EXIT_ENTER_TIMESTAMP,
    //                     CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as VALIDATED_IMAGE_URL,
    //                     IS_VALIDATED,
    //                     SCENE_NAME,
    //                     SMALL_CIRCLE_ID,
    //                     ORDER_INDEX,
    //                     VALIDATED_JOURNEY,
    //                     IS_BA,
    //                     true as IS_VALIDATED_FULL_JOURNEY
    //                 FROM backlight_samdt
    //                 WHERE small_circle_id in (${sm_ids.map(value => `'${value}'`).join(', ')});
    //             `, (err, result2) => {
    //                 res.status(200).json({
    //                     updatedData: result2,
    //                     message: "Success",
    //                 });
    //             })
    //         })
    //     })
    // } else {
    //     let fov_names = Object.keys(payload.small_circle_ids)
    //     let sm_ids = fov_names.map(d => payload.small_circle_ids[`${d}`])
    //     snowflake_client.execute_query(`
    //         UPDATE backlight_samdt
    //         SET VALIDATED_JOURNEY = '${payload.selected_data.JOURNEY_ID}', IS_VALIDATED = true, BA_TYPE = '${eventType}'
    //         WHERE small_circle_id in (${sm_ids.map(value => `'${value}'`).join(', ')})
    //     `, (err, result) => {
    //         if (err) {
    //             console.log('error executing snowflake query -', err);
    //         }
    //         snowflake_client.execute_query(`
    //             SELECT
    //                 JOURNEY_ID,
    //                 BA_TYPE,
    //                 ENTER_TIMESTAMP,
    //                 EXIT_TIMESTAMP,
    //                 CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as IMAGE_URL,
    //                 ENTER_TIMESTAMP as VALIDATED_ENTER_TIMESTAMP,
    //                 EXIT_TIMESTAMP as EXIT_ENTER_TIMESTAMP,
    //                 CONCAT(IMAGE_URL, '${process.env.SAMDT_SAS_TOKEN}') as VALIDATED_IMAGE_URL,
    //                 IS_VALIDATED,
    //                 SCENE_NAME,
    //                 SMALL_CIRCLE_ID,
    //                 ORDER_INDEX,
    //                 VALIDATED_JOURNEY,
    //                 IS_BA,
    //                 true as IS_VALIDATED_FULL_JOURNEY
    //             FROM backlight_samdt
    //             WHERE small_circle_id in (${sm_ids.map(value => `'${value}'`).join(', ')});
    //         `, (err, result2) => {
    //             res.status(200).json({
    //                 updatedData: result2,
    //                 message: "Success",
    //             });
    //         })
    //     })
    // }
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
                    CONCAT(
                        COALESCE(GNC_RESULT, IMAGE_URL),
                        CASE 
                            WHEN GNC_RESULT IS NOT NULL THEN '${process.env.SAMDT_SAS_TOKEN}'
                            ELSE '${process.env.SAMDT_SAS_TOKEN_AXIS}'
                        END
                    ) AS IMAGE_URL,
                    IS_VALIDATED,
                    SCENE_NAME,
                    SMALL_CIRCLE_ID,
                    ORDER_INDEX,
                    BBOX,
                    FINAL_BBOX,
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
                    CONCAT(
                        COALESCE(GNC_RESULT, IMAGE_URL),
                        CASE 
                            WHEN GNC_RESULT IS NOT NULL THEN '${process.env.SAMDT_SAS_TOKEN}'
                            ELSE '${process.env.SAMDT_SAS_TOKEN_AXIS}'
                        END
                    ) AS IMAGE_URL,
                    IS_VALIDATED,
                    SCENE_NAME,
                    SMALL_CIRCLE_ID,
                    ORDER_INDEX,
                    BBOX,
                    FINAL_BBOX,
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

    snowflake_client.execute_query(generatedQuery, (err, rows) => {
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