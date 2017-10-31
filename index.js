/************************************************************************
 *
 * HIGHLY CUSTOMIZABLE AUTO-MAPPING EXTENSION FOR ALOOMA ETL PLATFORM
 *
 * @Author: oakromulo (Romulo-FanHero)
 * @Date:   2017-10-27T19:47:19-02:00
 * @Email:  romulo@fanhero.com
 * @Last modified by:   oakromulo
 * @Last modified time: 2017-10-31T10:52:23-02:00
 * @License: MIT
 *
 ************************************************************************/

// dependencies
const _ = require('lodash');
const promise = require('bluebird');
const decamelize = require('decamelize');
const request = require('request-promise').defaults({ jar: true, json: true }); // <-- VERY HANDY!

// alooma access credentials
const EMAIL = process.env.ALOOMA_EMAIL;
const PASSWORD = process.env.ALOOMA_PASSWORD;
const BASE_URL = 'https://app.alooma.com:443/rest';

// table destination on the target (output) warehouse
const TARGET_SCHEMA = 'dataflux';

// default mapping mode for new mappings
const DEFAULT_MAPPING_MODE = 'STRICT';

// default settings to be applied to all fields identified by auto-map as VARCHAR
const DEFAULT_VARCHAR_LENGTH = 4096;
const DEFAULT_VARCHAR_TRUNCATION = true;

// choose between TIMESTAMP and TIMESTAMPTZ as default type for timestamp columns
const DEFAULT_TIMESTAMP_TYPE = 'TIMESTAMPTZ';

// define settings for the primary key (informative-only on Redshift)
const DEFAULT_PRIMARY_KEY = 'message_id';
const DEFAULT_PRIMARY_KEY_TYPE = 'CHAR';
const DEFAULT_PRIMARY_KEY_LENGTH = 36;
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
const FORCE_VARCHAR_PATTERNS = ['_id', 'version', 'timezone', 'build'];

// large numeric fields in this pattern are cast into BIGINT
const FORCE_BIGINT_PATTERNS = ['geolocation_timestamp'];

// patterns to identify fields that should be set as FLOATING POINT by default (e.g. IMU/GPS data)
const FORCE_FLOAT_PATTERNS = ['geolocation'];

// columns in this pattern are discarded (not mapped)
const DISCARD_COLUMN_PATTERNS = ['password', 'floor_level', 'integrations', '__c'];

// minimum number of times that a field must appear globally without being discarded due to rarity
const MIN_OCCURRENCE = 5; // 5 samples overall

// minimum relative occurence of a field relative to the sample size
const MIN_OCCURRENCE_PERCENT = 1.0; // 1 in 1000

// minimum number of distinct sample values for each field, bypassed if value chosen is less than 2
const MIN_DISTINCT_SAMPLES = 2; // 2 distinct samples overall

// maximum % of a particular sample value relative to the total samples of that particular field
const MAX_SAMPLE_OCCURRENCE_PERCENT = 98.9; // 989 in 1000

// patterns on table names to be bypassed and not processed at all by this script
const EVENT_EXCLUSION_PATTERN = ['develop', 'other'];

// naïve helper method to check if `str` include any of the specified `patterns`
const inPattern = (str, patterns) => {
    var found = false;
    patterns.forEach(p => {
        if (!found && str.toLowerCase().includes(p)) found = true;
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
        evts.filter(e => e.state === 'UNMAPPED' && !inPattern(e.name, EVENT_EXCLUSION_PATTERN)),

        // load full data on each type
        evt => request.get(`${BASE_URL}/event-types/${evt.name}`)

            // run alooma's vanilla auto-mapper
            .then(evt => request.post(`${BASE_URL}/event-types/${evt.name}/auto-map`, { json: evt }))

            // process fields, create table and commit new mapping on each automapped event type
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

                    // verify field stats to account for field acceptance criteria
                    var s1 = 0, s2 = 0, s3 = 0;
                    for (var k in f.stats) {
                        if (!f.stats[k] || !f.stats[k].count) continue;
                        s1 += f.stats[k].count;
                        if (!f.stats[k].samples) continue;
                        for (var l in f.stats[k].samples) {
                            if (!f.stats[k].samples[l] || !f.stats[k].samples[l].count) continue;
                            s2++;
                            s3 = f.stats[k].samples[l].count > s3 ? f.stats[k].samples[l].count : s3;
                        }
                    }

                    // process non-meta fields
                    var meta = f.mapping.columnName.includes('_metadata');
                    if (!meta) {

                        // specifiy column name
                        f.mapping.columnName = f.father + fixNaming(f.fieldName);
                        delete f.father;

                        // verify discard conditions and set flag accordingly
                        f.mapping.isDiscarded = (
                            (s1 && ((s1 < MIN_OCCURRENCE) || ((s1 * 100.0 / evt.stats.count) < MIN_OCCURRENCE_PERCENT))) ||
                            (s1 && s3 && (s3 * 100.0 / s1 > MAX_SAMPLE_OCCURRENCE_PERCENT)) ||
                            (s2 && (s2 < MIN_DISTINCT_SAMPLES)) ||
                            inPattern(f.mapping.columnName, DISCARD_COLUMN_PATTERNS)
                        )  && f.mapping.columnName !== DEFAULT_PRIMARY_KEY && f.mapping.columnName !== DEFAULT_DISTRIBUTION_KEY;

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

                        // enforce defawult timestamp behavior
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

                    // clear columnName and columnType for discarded fields
                    if (f.mapping.isDiscarded) {
                        f.mapping.columnName = '';
                        f.mapping.columnType = null;
                    }
                    else mappings.push(_.omit(_.cloneDeep(f.mapping), ['isDiscarded', 'machineGenerated', 'subFields']));
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

                // create table on the target schema then apply custom mapping
                return request.post(`${BASE_URL}/tables/${TARGET_SCHEMA}/${evt.name}`, { json: mappings })
                    .then(() => request.post(`${BASE_URL}/event-types/${finalEvt.name}/mapping`, { json: {
                        name: evt.name,
                        mapping: {
                            tableName: evt.name,
                            schema: TARGET_SCHEMA,
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
