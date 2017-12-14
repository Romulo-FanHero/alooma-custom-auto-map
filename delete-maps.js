// dependencies
const promise = require('bluebird');
const request = require('request-promise').defaults({ jar: true, json: true }); // <-- VERY HANDY!

// alooma access credentials
const EMAIL = process.env.ALOOMA_EMAIL;
const PASSWORD = process.env.ALOOMA_PASSWORD;
const BASE_URL = 'https://app.alooma.com:443/rest';

// login, list events, check deletion pattern, delete events in pattern
request.post(`${BASE_URL}/login`, { json: { email: EMAIL, password: PASSWORD } })
    .then(() => request.get(`${BASE_URL}/event-types`))
    .then(evts => promise.map(
        evts.filter(e => e.state === 'MAPPED' && e.name.includes('production')),
        evt => request.delete(`${BASE_URL}/event-types/${evt.name}`).catch(console.error))
    )
    .catch(console.error);
