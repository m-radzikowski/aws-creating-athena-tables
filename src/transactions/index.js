const {Firehose} = require('@aws-sdk/client-firehose');
const {v4: uuidv4} = require('uuid');
const {DateTime} = require('luxon');

const firehose = new Firehose({apiVersion: '2015-08-04'});
const textEncoder = new TextEncoder();

exports.handler = async () => {
    const now = DateTime.utc();
    const records = new Array(20).fill(undefined).map(() => ({
        id: uuidv4(),
        product_id: now.toFormat('yyyyMMddHHmm') + '-' + Math.floor(Math.random() * 5),
        created_at: now.toFormat('yyyy-MM-dd HH:mm:ss'),
    }));

    await firehose.putRecordBatch({
        DeliveryStreamName: process.env.FIREHOSE_NAME,
        Records: records.map(r => ({
            Data: textEncoder.encode(JSON.stringify(r) + '\n')
        })),
    });
};
