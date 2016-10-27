'use strict';

const Stream = require('stream');

class ReadableStream extends Stream.Readable {
  constructor(options) {
    super(options);
    this._data = '';
  }

  _read() {
    const ret = this.push(this._data);
    this._data = '';
    return ret;
  }

  append(data) {
    this._data = data;
    this.read(0);
  }

  end() {
    this.push(null);
  }
}

module.exports = ReadableStream;
