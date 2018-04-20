"use strict";

const AWS = require("aws-sdk");
const KinesisStream = require("../kinesis-stream.js");

function Streams(options) {
  this.cache = new Map();
  this.streams = new Map();
  const opts = Object.assign({}, options);
  let kinesisClientOptions = {
    apiVersion: "2013-12-02",
    region: opts.kinesis.region,
    sslEnabled: opts.kinesis.sslEnabled,
    retryDelayOptions: {
      base: 100
    }
  };

  if (opts.kinesis.accessKeyId) {
    kinesisClientOptions.accessKeyId = opts.kinesis.accessKeyId;
  }

  if (opts.kinesis.secretAccessKey) {
    kinesisClientOptions.secretAccessKey = opts.kinesis.secretAccessKey;
  }

  if (opts.kinesis.endpoint) {
    kinesisClientOptions.endpoint = opts.kinesis.endpoint;
  }

  if (opts.streams) {
    for (const stream of opts.streams) {
      const kinesisStreamOptions = {
        partitionKey: stream.partitionKey,
        partitionKeyProperty: stream.partitionKeyProperty,
        maxBytes: stream.maxBytes,
        msFlushRate: stream.msFlushRate,
        kinesisClient: new AWS.Kinesis(kinesisClientOptions),
        streamName: stream.streamName,
        maxRetries: stream.maxRetries
      };

      this.addStreamPatterns(new KinesisStream(kinesisStreamOptions), stream.files);
    }
  }
}

Streams.prototype.destroy = function() {
  for (const stream of this.getStreams()) {
    stream.stop();
  }
}

Streams.prototype.addStreamPatterns = function(stream, patterns) {
  if (!this.streams.get(stream)) {
    this.streams.set(stream, new Set());
  }

  const current = this.streams.get(stream);
  for (const pattern of patterns) {
    current.add(new RegExp(pattern));
  }
};

Streams.prototype.getStreams = function() {
  return new Set(this.streams.keys());
};

Streams.prototype.getRoutes = function(file) {
  let streams = this.cache.get(file);
  if (streams) {
    return streams;
  }

  streams = new Set();
  for (const [stream, regexps] of this.streams.entries()) {
    for (const regexp of regexps) {
      if (regexp.test(file)) {
        streams.add(stream);
      }
    }
  }

  this.cache.set(file, streams);
  return streams;
};

module.exports = Streams;
