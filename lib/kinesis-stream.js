"use strict";

const bunyan = require("bunyan");
const EventEmitter = require("events");
const uuidV4 = require("uuid/v4");

const log = bunyan.createLogger({
  name: "seguir-kinesis",
  file: "kinesis-stream.js"
});

function KinesisStream(options) {
  const opts = Object.assign({}, options);
  this.emitter = new EventEmitter();
  this.emitter.on("buffer.full", this.flush.bind(this));

  if (typeof opts.maxRecords !== "undefined") {
    this.maxRecords = opts.maxRecords;
  } else {
    this.maxRecords = 500;
  }

  if (typeof opts.maxBytes !== "undefined") {
    this.maxBytes = opts.maxBytes;
  } else {
    this.maxBytes = 5242880;
  }

  if (typeof opts.maxRetries !== "undefined") {
    this.maxRetries = opts.maxRetries;
  } else {
    this.maxRetries = 3;
  }

  if (typeof opts.msFlushRate !== "undefined") {
    this.msFlushRate = opts.msFlushRate;
  } else {
    this.msFlushRate = 60000;
  }

  this.partitionKey = opts.partitionKey;
  this.partitionKeyProperty = opts.partitionKeyProperty;
  this.kinesisClient = opts.kinesisClient;
  this.streamName = opts.streamName;
  this.buffer = [];
  this.size = 0;

  this.interval = undefined;
  if (this.msFlushRate > 0) {
    this.interval = setInterval(this.scheduledFlush.bind(this), this.msFlushRate)
  }
}

KinesisStream.prototype.write = function(line) {
  const record = this.convertToRecord(line);
  const sizeOf = this.sizeOf(record);
  if (sizeOf > 1048576) {
    log.warn({
      recordSizeInBytes: sizeOf
    }, "Record data and partition key size combined were greater than 1MB. Ignoring.");

    return;
  }

  if ((this.buffer.length + 1) > this.maxRecords || ((this.size + sizeOf > this.maxBytes) && this.size !== 0)) {
    this.emitter.emit("buffer.full", this.buffer, this.size);
    this.buffer = [];
    this.size = 0;
  }

  this.buffer.push(record);
  this.size += sizeOf;
};

KinesisStream.prototype.convertToRecord = function(line) {
  return {
    Data: line,
    PartitionKey: this.getPartitionKey(line)
  };
};

KinesisStream.prototype.getPartitionKey = function(line) {
  if (this.partitionKeyProperty) {
    const obj = JSON.parse(line);
    let partitionKey = obj[this.partitionKeyProperty];
    if (typeof partitionKey === "undefined") {
      log.warn({
        partitionKeyProperty: this.partitionKeyProperty,
        data: obj
      }, "The specified partitionKeyProperty was undefined.");

      partitionKey = "undefined";
    }

    if (typeof partitionKey !== "string") {
      log.warn({
        partitionKeyProperty: this.partitionKeyProperty,
        partitionKey: partitionKey,
        data: obj
      }, "The retrieved partitionKey was not a string.");

      partitionKey = JSON.stringify(partitionKey);
    }

    return partitionKey;
  }

  return this.partitionKey || uuidV4();
};

KinesisStream.prototype.scheduledFlush = function() {
  if (this.buffer.length == 0) {
    return;
  }

  this.flush(this.buffer, this.size);
  this.buffer = [];
  this.size = 0;
};

KinesisStream.prototype.flush = function(records, size) {
  const batchId = uuidV4();
  log.info({
    batchSizeInRecords: this.buffer.length,
    batchSizeInBytes: this.size,
    batchId: batchId
  }, "Sending batch to Kinesis.");

  this.kinesisClient.putRecords({
    Records: records,
    StreamName: this.streamName
  }, this.callback.bind(this, batchId, records, 0));
};

KinesisStream.prototype.callback = function(batchId, records, attempt, err, data) {
  if (err) {
    log.error(err, "An unexpected exception was caught.");
    return;
  }

  if (data.FailedRecordCount > 0 && attempt < this.maxRetries) {
    let failedRecords = [];
    data.Records.forEach((record, index) => {
      if (record.ErrorCode) {
        const actual = records[index];
        failedRecords.push(actual);
        log.error({
          errorCode: record.ErrorCode,
          errorMessage: record.ErrorMessage,
          record: actual,
          batchId: batchId,
          requestId: data.requestId
        }, "Record failed to be delivered successfully.");
      }
    });

    this.kinesisClient.putRecords({
      Records: failedRecords,
      StreamName: this.streamName
    }, this.callback.bind(this, batchId, failedRecords, attempt + 1));
  } else {
    log.info({
      batchId: batchId,
      retries: attempt,
      failed: (attempt == this.maxRetries)
    }, "Finished sending batch to Kinesis.");
  }
};

KinesisStream.prototype.stop = function() {
  if (typeof this.interval !== "undefined") {
    clearInterval(this.interval);
  }

  this.emitter.removeListener("buffer.full", this.flush);
};

KinesisStream.prototype.sizeOf = function(record) {
  return Buffer.byteLength(record.Data + record.PartitionKey);
};

module.exports = KinesisStream;
