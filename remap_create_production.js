
// dependencies
const _ = require('lodash');
const promise = require('bluebird');
const decamelize = require('decamelize');
const request = require('request-promise').defaults({ jar: true, json: true }); // <-- VERY HANDY!

// alooma access credentials
const EMAIL = process.env.ALOOMA_EMAIL;
const PASSWORD = process.env.ALOOMA_PASSWORD;
const BASE_URL = 'https://app.alooma.com:443/rest';

// default mapping mode for new mappings
const DEFAULT_MAPPING_MODE = 'STRICT';

// default settings to be applied to all fields identified by auto-map as VARCHAR
const DEFAULT_VARCHAR_LENGTH = 4096;
const DEFAULT_VARCHAR_TRUNCATION = true;

// choose between TIMESTAMP and TIMESTAMPTZ as default type for timestamp columns
const DEFAULT_TIMESTAMP_TYPE = 'TIMESTAMPTZ';

// define settings for the primary key (informative-only on Redshift)
const DEFAULT_PRIMARY_KEY = '_id';
const DEFAULT_PRIMARY_KEY_TYPE = 'VARCHAR';
const DEFAULT_PRIMARY_KEY_LENGTH = 64;
const DEFAULT_PRIMARY_KEY_TRUNCATION = false;

// define a column for an evenly distribution of fact table data across nodes
const DEFAULT_DISTRIBUTION_KEY = 'timestamp';

// default datatype settings for fields identified as `id` columns
const DEFAULT_ID_TYPE = 'VARCHAR';
const DEFAULT_ID_LENGTH = 256;
const DEFAULT_ID_TRUNCATION = false;
const ID_PATTERNS = ['id'];

// specify custom length for sortkeys with datatype varchar
const DEFAULT_SORT_KEY_VARCHAR_LENGTH = 256;

// determine patterns that indicate that a column is a good candidate for a compound/interleaved sortkey
const SORT_KEY_PATTERNS = ['timestamp', 'id', 'user', 'email', 'gender', 'os_name', 'birthday', 'created_at'];

// high-dispersion numeric fields that fit within the pattern below are cast into varchar
const FORCE_VARCHAR_PATTERNS = ['_id', 'version', 'timezone', 'build', 'floor_level', 'google_analytics'];

// large numeric fields in this pattern are cast into BIGINT
const FORCE_BIGINT_PATTERNS = ['geolocation_timestamp', 'properties_transaction'];

// patterns to identify fields that should be set as FLOATING POINT by default (e.g. IMU/GPS data)
const FORCE_FLOAT_PATTERNS = ['geolocation'];

// patterns on table names to be bypassed and not processed at all by this script
const EVENT_EXCLUSION_PATTERN = ['develop', 'other'];

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

    // process all unmapped event-types
    .then(evts => promise.map(

        // filter unmapped types
        evts.filter(e => e.state === 'UNMAPPED' && !inPattern(e.name, EVENT_EXCLUSION_PATTERN) && e.name.includes('production.')),

        // load full data on each type
        evt => request.get(`${BASE_URL}/event-types/${evt.name}`)

            // run alooma's vanilla auto-mapper
            .then(evt => request.post(`${BASE_URL}/event-types/${evt.name}/auto-map`, { json: evt }))

            // process fields and commit new mapping on each automapped event type
            .then(evt => {

                console.log(evt.name, 'started');

                // flat list to store the mapping for each field on the evt obj
                var mappings = [];

                // initial sort key index
                var sortKeyIndex = 0;

                // recursive function to iterate all fields and sub-fieldsm, applying a custom auto-map
                var autoMap = e => e.fields.forEach(f => {

                    // fill parent fields
                    if (!e.father) e.father = '';
                    if (!e.fieldName) e.fieldName = '';

                    f.father = e.father + fixNaming(e.fieldName);
                    if (f.father) f.father += '_';

                    // continue iterating if not a root event
                    if (f.fields.length > 0) return autoMap(f);

                    // process non-meta fields
                    if (!(f.mapping.columnName.includes('_metadata') || f.father.includes('_metadata') || f.fieldName.includes('_metadata') || (f.father + fixNaming(f.fieldName)).includes('_metadata'))) {

                        // specifiy column name
                        f.mapping.columnName = f.father + fixNaming(f.fieldName);
                        delete f.father;

                        // verify discard conditions and set flag accordingly
                        f.mapping.isDiscarded = false;

                        // check if column type matches a data type change pattern
                        if (inPattern(f.mapping.columnName, FORCE_FLOAT_PATTERNS)) {
                            f.mapping.columnType.type = 'FLOAT_NORM';
                        }
                        if (inPattern(f.mapping.columnName, FORCE_BIGINT_PATTERNS)) {
                            f.mapping.columnType.type = 'BIGINT';
                        }
                        if (inPattern(f.mapping.columnName, FORCE_VARCHAR_PATTERNS)) {
                            f.mapping.columnType.type = 'VARCHAR';
                        }

                        // enforce default varchar behavior
                        if (f.mapping.columnType.type === 'VARCHAR') {
                            f.mapping.columnType.length = DEFAULT_VARCHAR_LENGTH;
                            f.mapping.columnType.truncate = DEFAULT_VARCHAR_TRUNCATION;
                        }

                        // enforce default timestamp behavior
                        if (f.mapping.columnType.type.toLowerCase().includes('timestamp')) {
                            f.mapping.columnType.type = DEFAULT_TIMESTAMP_TYPE;
                        }

                        // enforce default settings for ID fields
                        if (inPattern(f.mapping.columnName, ID_PATTERNS)) {
                            f.mapping.columnType.type = DEFAULT_ID_TYPE;
                            f.mapping.columnType.length = DEFAULT_ID_LENGTH;
                            f.mapping.columnType.truncate = DEFAULT_ID_TRUNCATION;
                        }

                        // set sort key if column within pattern
                        if (inPattern(f.mapping.columnName, SORT_KEY_PATTERNS)) {
                            f.mapping.sortKeyIndex = sortKeyIndex++;
                            if (f.mapping.columnType.type === 'VARCHAR') {
                                f.mapping.columnType.length = DEFAULT_SORT_KEY_VARCHAR_LENGTH;
                            }
                        }
                        else f.mapping.sortKeyIndex = -1;

                        // set distribution key if dist column
                        if (f.mapping.columnName === DEFAULT_DISTRIBUTION_KEY) {
                            f.mapping.distKey = true;
                        }
                        else f.mapping.distKey = false;

                        // set primary key if pk column
                        if (f.mapping.columnName === DEFAULT_PRIMARY_KEY) {
                            f.mapping.columnType.type = DEFAULT_PRIMARY_KEY_TYPE;
                            f.mapping.columnType.length = DEFAULT_PRIMARY_KEY_LENGTH;
                            f.mapping.columnType.truncate = DEFAULT_PRIMARY_KEY_TRUNCATION;
                            f.mapping.columnType.nonNull = true;
                            f.mapping.primaryKey = true;
                        }
                        else f.mapping.primaryKey = false;

                        // enforce no length/truncate fields on non-CHAR types
                        if (!f.mapping.columnType.type.toLowerCase().includes('char')) {
                            delete f.mapping.columnType.length;
                            delete f.mapping.columnType.truncate;
                        }
                    }

                    // metafields should NEVER be discarded
                    else {
                        delete f.father;

                        if (!f.mapping) f.mapping = {};
                        f.mapping.isDiscarded = false;

                        if (!f.mapping.columnName) {
                            f.mapping.columnName = `_metadata_${fixNaming(f.fieldName)}`;
                        }

                        if (!f.mapping.columnType) {
                            f.mapping.columnType = { nonNull: false };
                        }

                        if (
                            (f.mapping.columnName && f.mapping.columnName.includes('_object')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('_object')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('_url')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('_url')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('_id')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('_id')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('uuid')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('uuid')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('input')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('input')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('type')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('type')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('database')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('database')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('db')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('db')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('collection')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('collection')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('table')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('table')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('schema')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('schema')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('token')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('token')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('version')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('version')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('client')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('client'))
                        ) {
                            f.mapping.columnType.type = 'VARCHAR';
                            f.mapping.columnType.length = 1024;
                            f.mapping.columnType.truncate = false;
                        }

                        if (
                            (f.mapping.columnName && f.mapping.columnName.includes('timestamp')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('timestamp')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('updated')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('updated')) ||

                            (f.mapping.columnName && f.mapping.columnName.includes('pull_time')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('pull_time'))
                        ) {
                            f.mapping.columnType.type = 'TIMESTAMP';
                        }

                        if (
                            (f.mapping.columnName && f.mapping.columnName.includes('deleted')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('deleted'))
                        ) {
                            f.mapping.columnType.type = 'BOOLEAN';
                        }

                        if (
                            (f.mapping.columnName && f.mapping.columnName.includes('restream_count')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('restream_count')) ||
                            (f.mapping.columnName && f.mapping.columnName.includes('ordinal')) ||
                            (f.mapping.fieldName && f.mapping.fieldName.includes('ordinal'))
                        ) {
                            f.mapping.columnType.type = 'BIGINT';
                        }
                    }

                    mappings.push(_.omit(_.cloneDeep(f.mapping), ['isDiscarded', 'machineGenerated', 'subFields']));
                });
                autoMap(evt);

                // exect recursive cleanup routine to discard extra fields not required for mapping
                var cleanUp = e => e.fields.forEach(f => {
                    try { delete f.father; } catch (ex) {}
                    try { delete f.fields.father; } catch (ex) {}
                    try { delete f.stats; } catch (ex) {}
                    try { delete f.fields.stats; } catch (ex) {}
                    try { delete f.mapping.sortKeyIndex; } catch (ex) {}
                    try { delete f.mapping.distKey; } catch (ex) {}
                    try { delete f.mapping.primaryKey; } catch (ex) {}
                    try { delete e.father; } catch (ex) {}
                    try { delete e.fields.father; } catch (ex) {}
                    try { delete e.stats; } catch (ex) {}
                    try { delete e.fields.stats; } catch (ex) {}
                    try { delete e.mapping.sortKeyIndex; } catch (ex) {}
                    try { delete e.mapping.distKey; } catch (ex) {}
                    try { delete e.mapping.primaryKey; } catch (ex) {}
                    if (f.fields.length > 0) return cleanUp(f);
                });
                cleanUp(evt);

                const schema = evt.name.split('.')[0];
                const tableName = evt.name.split('.')[1];

                // apply custom mapping
                return request.post(`${BASE_URL}/tables/${schema}/${tableName}`, { json: mappings })
                    .then(() => request.post(`${BASE_URL}/event-types/${evt.name}/mapping`, { json: {
                        name: evt.name,
                        mapping: {
                            tableName: tableName,
                            schema: schema
                        },
                        fields: evt.fields,
                        mappingMode: DEFAULT_MAPPING_MODE
                    }}))
                    .then(() => console.log(evt.name, 'finished'))
                    .catch(console.error);
            })
            .catch(console.error)
    ))
    .catch(console.error);
