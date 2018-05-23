// dependencies
const _ = require('lodash');
const promise = require('bluebird');
const decamelize = require('decamelize');
const request = require('request-promise').defaults({ jar: true, json: true }); // <-- VERY HANDY!

// alooma access credentials
const EMAIL = process.env.ALOOMA_EMAIL;
const PASSWORD = process.env.ALOOMA_PASSWORD;
const BASE_URL = 'https://app.alooma.com:443/rest';

const BLACKLIST = ['age', 'avatar', 'birthday', 'currency', 'email', 'fb', 'gender', 'itunes', 'language', 'locale', '_location', 'name', 'password', 'signal', 'store', 'timezone', 'token'];

// naïve helper method to check if `str` include any of the specified `patterns`
const inPattern = (str, patterns) => {
    var found = false;
    patterns.forEach(p => {
        found = !found && str.toLowerCase().includes(p.toLowerCase()) ? true : found;
    });
    return found;
};

// helper function to fix column names by decamelizing and replacing spaces with underscores
const fixNaming = n => decamelize(n).replace(/ /g, '_');

// login (auth cookie is picked up here automatically and propagates to all subsequent calls)
request.post(`${BASE_URL}/login`, { json: { email: EMAIL, password: PASSWORD } })

    // get all event-types
    .then(() => request.get(`${BASE_URL}/event-types`))

    // process all mapped dataflux event-types
    .then(evts => promise.map(

        // filter unmapped types
        evts.filter(e => e.state === 'MAPPED' && e.name.includes('dataflux') && !e.name.includes('clicked_track_event')),

        // load full data on each type
        evt => request.get(`${BASE_URL}/event-types/${evt.name}`)

            // process fields and commit new mapping on each automapped event type
            //
            .then(evt => {

                console.log(evt.name, 'started');

                var cleanUp = e => e.fields.forEach(f => {
                    try { delete f.father; } catch (ex) {}
                    try { delete f.fields.father; } catch (ex) {}
                    try { delete f.stats; } catch (ex) {}
                    try { delete f.fields.stats; } catch (ex) {}
                    //try { delete f.mapping.sortKeyIndex; } catch (ex) {}
                    //try { delete f.mapping.distKey; } catch (ex) {}
                    //try { delete f.mapping.primaryKey; } catch (ex) {}
                    try { delete e.father; } catch (ex) {}
                    try { delete e.fields.father; } catch (ex) {}
                    try { delete e.stats; } catch (ex) {}
                    try { delete e.fields.stats; } catch (ex) {}
                    //try { delete e.mapping.sortKeyIndex; } catch (ex) {}
                    //try { delete e.mapping.distKey; } catch (ex) {}
                    //try { delete e.mapping.primaryKey; } catch (ex) {}
                    if (f.fields.length > 0) return cleanUp(f);
                });

                var autoMap = e => e.fields.forEach(f => {

                    // continue iterating if not a root event
                    if (f.fields.length > 0) return autoMap(f);

                    if (f.mapping && _.has(f.mapping, 'columnName') && f.mapping.columnName && inPattern(f.mapping.columnName, BLACKLIST) && inPattern(f.mapping.columnName, ['traits']) && !inPattern(f.mapping.columnName, ['meta'])) {
                        f.mapping.isDiscarded = true;
                        f.mapping.columnType = null;
                        f.mapping.columnName = '';
                        f.mapping.machineGenerated = false;
                    }
                });

                cleanUp(evt);
                autoMap(evt);
                cleanUp(evt);

                // overwrite mapping
                return request.post(`${BASE_URL}/event-types/${evt.name}/mapping`, { json: {
                        name: evt.name,
                        mapping: {
                            tableName: evt.name.split('.')[1],
                            schema: evt.name.split('.')[0]
                        },
                        fields: evt.fields,
                        mappingMode: 'STRICT'
                    }})
                    .then(() => console.log(evt.name, 'finished'))
                    .catch(() => console.error(evt.name, 'failed'));
                    //.catch(console.error);
            })
            .catch(console.error)
    ))
    .catch(console.error);
