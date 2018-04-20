"use strict";

const Streams = require("./lib/handlers/streams.js");
const config = require("./lib/handlers/config.js");

function SeguirKinesis(options, eventEmitter) {
  this.eventEmitter = eventEmitter;
  const opts = config.parse(options);
  this.streams = new Streams(opts);
}

SeguirKinesis.prototype.handler = function(line, file) {
  this.streams.getRoutes(file).forEach((stream) => {
    stream.write(line);
  });
};

SeguirKinesis.prototype.start = function() {
  this.eventEmitter.on("line", this.handler.bind(this));
};

SeguirKinesis.prototype.stop = function(cb) {
  this.eventEmitter.removeListener("line", this.handler);
  this.streams.destroy();
  cb();
};

module.exports = SeguirKinesis;
