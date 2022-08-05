require('dotenv').config();
const axios = require("axios");
const { QueryTypes } = require('sequelize');
const moment = require('moment');
const { BlobServiceClient, StorageSharedKeyCredential } = require("@azure/storage-blob");
const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;

module.exports = async () => {
    const db = require('../database/viana');
    await db.postgres.query(
    `
        SELECT
            ss.id,
            ss.timestamp_str,
            ss.api_key,
            ss.serial_id,
            ss.track_object,
            ss.image_url,
            ss."createdAt"
        FROM mqtt_detections ss
        WHERE
            ss.type ->> 'name' = 'Pandora' AND
            "createdAt" > '2022-06-03 00:00:00' AND
            ss."snapshot_generated" = false AND
            (ss.processing != true OR ss.processing IS NULL) AND
            (ss.retry_count < 50  OR ss.retry_count IS NULL) AND
            extract(epoch from (now()::timestamp -  "createdAt")) > 120
        ORDER BY "timestamp_str" ASC
        LIMIT 4
    `,
    {
        raw: true,
        type: QueryTypes.SELECT
    }
    ).then(async tracks => {
        if (tracks.length > 0) {
            await setProcessingStatus(tracks.map(d => d.id), true);
            Promise.allSettled(tracks.map(async ss => {
                let ts = ss.timestamp_str;
                let snapshot_timestamp = new Date(parseInt(ts)).toISOString() || null;

                return new Promise(async (resolve, reject) => {
                    const meraki_snapshot_api = `https://api.meraki.com/api/v1/devices/${ss.serial_id}/camera/generateSnapshot`;
                    let axiosConfig = {
                        headers: {
                            // "Content-Type": "application/json",
                            "Accept": "application/json",
                            "User-Agent": 'viana / 4.0 meldCX',
                            "X-Cisco-Meraki-API-Key": ss.api_key,
                        }
                    };

                    let option = { timestamp: snapshot_timestamp };
                    let imageUrl =  await axios.post(meraki_snapshot_api, option, axiosConfig)
                    .then(async function (response) {
                        return response.data.url;
                    })
                    .catch(function (error) {
                        console.log('pandora - 2', error);
                        setProcessingStatus([ss.id], false).then(d => {
                            let additionalMessage = error.response && error.response.data && error.response.data.errors || [];
                            console.log(`[${ss.id} - ${ss.createdAt}] ${error.message}, ${[...additionalMessage].join()}`);
                        });
                        return reject(error.message);
                    });

                    if (imageUrl && imageUrl !== (undefined || null)) {
                        await new Promise(resolve => setTimeout(resolve, 5000));

                        const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
                        const containerClient = blobServiceClient.getContainerClient(process.env.AZURE_STORAGE_CONTAINER_NAME);
                        const blobName = `${moment().format('MM-DD-YYYY')}/sb-snapshot-${ss.serial_id}-${snapshot_timestamp}.jpeg`
                        const blockBlobClient = containerClient.getBlockBlobClient(blobName);
                        axios({ // download image
                            method: 'get',
                            url: imageUrl,
                            responseType: 'arraybuffer'
                        }).then(async arraybuffer => {
                            blockBlobClient.upload(arraybuffer.data, Buffer.byteLength(arraybuffer.data));
                            await new Promise(resolve => setTimeout(resolve, 1000));     
                            
                            let attributes = { status: "paused" };
                            // let output = ss.track_object[0];
                            // try {
                            //     res = await axios.post(`${process.env.GNC_URL}/snapper_lpr_detection`, {
                            //         "id": 0,
                            //         "url": blockBlobClient.url,
                            //         "objects" :{
                            //             "oid": 1,
                            //             "x0" : output.location[0],
                            //             "x1" : output.location[1],
                            //             "y0" : output.location[2],
                            //             "y1" : output.location[3]
                            //         }
                            //     })
                            //     attributes = res.data.result;
                            // } catch (error) {
                            //     attributes = {
                            //         error: error.message
                            //     }
                            // }

                            await db.postgres.query(
                                `
                                    UPDATE mqtt_detections
                                    SET
                                        "updatedAt" = CURRENT_TIMESTAMP,
                                        snapshot_generated = true,
                                        attributes = '${JSON.stringify(attributes)}',
                                        image_url='${blockBlobClient.url}'
                                    where
                                        id = ${ss.id}
                                `,
                                {
                                    raw: true,
                                    type: QueryTypes.SELECT
                                }
                            ).then(async d => {
                                resolve('yehey')
                            }).catch(err => {
                                console.log('err', err.message);
                                reject(err.message);
                            })
                        }).catch(error => {
                            console.log('pandora - 1', error.message);
                            setProcessingStatus([ss.id], false).then(d => {
                                let additionalMessage = error.response && error.response.data && error.response.data.errors || [];
                                console.log(`[${ss.id} - ${ss.createdAt}] ${error.message}, ${[...additionalMessage].join()}`);
                            });
                        })
                    } else {
                        reject('no image')
                    }
                });
            }))
            .then((results) => {
                let fulfilled = results.filter(data => data.status === 'fulfilled');
                console.log(`PANDORA - ${fulfilled.length} out of ${tracks.length} request has ${fulfilled.length > 1 ? 'there' : 'its'} image${fulfilled.length > 1 ? 's' : ''} stored to database.`);
            })
        } else {
            console.log('No snapshot on PANDORA queue')
        }
    }).catch(err => {
        console.log('ERROR: ', err);
    });
}

let setProcessingStatus = (ids, processing, retry_count) => {
    const db = require('../database/viana');
    let retry_count_to_add = retry_count || 1;

    return new Promise((resolve, reject) => {
        db.postgres.query(
            `
              UPDATE mqtt_detections
              SET processing = ${processing ? true : false},
                  "updatedAt" = CURRENT_TIMESTAMP,
                  retry_count = CASE
                                  WHEN retry_count IS NULL
                                      THEN 1
                                      ELSE retry_count + ${processing ? 0 : retry_count_to_add}
                                END
              WHERE id IN (${ids})
          `,
            {
                raw: true,
                type: QueryTypes.SELECT
            }
        )
            .then(res => {
                resolve(res)
            })
            .catch(err => {
                reject(err.message)
            });
    });
}