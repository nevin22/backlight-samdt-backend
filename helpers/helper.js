const axios = require("axios");

const MERAKI_API_URL = 'https://api.meraki.com/api/v1/';//process.env.MERAKI_API_URL;

function formatHeader(api_key) {
    return {
        "X-Cisco-Meraki-API-Key": api_key
    }
}

function evaluateEndpoint(endpoint, params = null) {
    let _endpoint;
    switch (endpoint) {
        case 'organizations':
            _endpoint = 'organizations';
            break;
        case 'device_status':
            _endpoint = `organizations/${params}/devices/statuses`;
            break;
        case 'devices':
            _endpoint = `devices/${params}`;
            break;
        case 'networks':
            _endpoint = `networks/${params}`;
            break;
        default:
            break;
    }
    return _endpoint;
}

function meraki(parameters) {
    return new Promise(async (resolve, reject) => {
        const cisco_meraki_header = formatHeader(parameters.api_key);
        const endpoint = evaluateEndpoint(parameters.url_endpoint, parameters.params);

        try {
            let response = await axios.get(`${MERAKI_API_URL}/${endpoint}`, { headers: cisco_meraki_header });
            return resolve(response);
        } catch (error) {
            return reject(error)
        }
    });
}

exports.meraki = meraki;