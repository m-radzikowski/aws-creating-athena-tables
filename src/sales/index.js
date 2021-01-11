const {Athena} = require('@aws-sdk/client-athena');
const {Glue} = require('@aws-sdk/client-glue');
const {DateTime} = require('luxon');

const athena = new Athena({apiVersion: '2017-05-18'});
const glue = new Glue({apiVersion: '2017-03-31'});

const bucketName = process.env.BUCKET_NAME;
const databaseName = process.env.DATABASE_NAME;
const tableName = 'sales';

exports.handler = async () => {
    // query data from 3 minutes ago so both Firehose and Glue crawler
    // for given minute already finished
    const datetime = DateTime.utc().minus({minutes: 3});

    const tableExists = await tableExists(databaseName, tableName);
    const createOrInsert = tableExists ?
        buildInsertInto(databaseName, tableName) :
        buildCreateTable(databaseName, tableName, bucketName);

    const query = `
        ${createOrInsert}
        ${buildQueryBody(databaseName, datetime)}
    `;
    await athena.startQueryExecution({
        QueryString: query,
        ResultConfiguration: {
            OutputLocation: `s3://${bucketName}/queries/`
        }
    });
};

const tableExists = async (database, table) => {
    try {
        await glue.getTable({
            DatabaseName: database,
            Name: table,
        });
        return true;
    } catch (e) {
        if (e.name === 'EntityNotFoundException') {
            return false;
        } else {
            throw e;
        }
    }
};

const buildInsertInto = (database, table) =>
    `INSERT INTO "${database}"."${table}"`;

const buildCreateTable = (database, table, bucket) => `
    CREATE TABLE "${database}"."${table}"
    WITH (
        external_location = 's3://${bucket}/${table}',
        format = 'JSON'
    ) AS`;

const buildQueryBody = (database, dt) => `
    SELECT
        p.id AS product_id,
        COUNT(t.id) AS count
    FROM "${database}"."products" p
    JOIN "${database}"."transactions" t ON p.id = t.product_id
    WHERE
        p.year = '${dt.toFormat('yyyy')}' AND p.month = '${dt.toFormat('MM')}' AND p.day = '${dt.toFormat('dd')}'
        AND p.hour = '${dt.toFormat('HH')}' AND p.minute = '${dt.toFormat('mm')}'
        AND t.datetime = '${dt.toFormat('yyyy/MM/dd/HH/mm')}'
    GROUP BY p.id`;
