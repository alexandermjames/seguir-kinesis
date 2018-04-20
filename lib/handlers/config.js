"use strict";

const Ajv = require("ajv");
const bunyan = require("bunyan");
const schema = require("../schema/schema.json");

const ajv = new Ajv();
const log = bunyan.createLogger({
  name: "seguir-kinesis",
  file: "config-handler.js"
});

const parse = (options) => {
  let context = Object.assign({}, options);
  if (!ajv.validate(schema, context)) {
    throw new Error(ajv.errorsText());
  }

  if (!context.kinesis.region) {
    context.kinesis.region = process.env.AWS_REGION;
  }

  if (!context.kinesis.accessKeyId) {
    context.kinesis.accessKeyId = process.env.AWS_ACCESS_KEY_ID;
  }

  if (!context.kinesis.secretAccessKey) {
    context.kinesis.secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
  }

  return context;
};

module.exports = {
  parse: parse
};
