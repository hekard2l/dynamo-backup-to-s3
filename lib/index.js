'use strict';

const _ = require('lodash');
const AWS = require('aws-sdk');
const events = require('events');
const moment = require('moment');
const dateRange = require('moment-range');
const path = require('path');
const async = require('async');
const util = require('util');

const Uploader = require('s3-streaming-upload').Uploader;
const ReadableStream = require('./readable-stream');
const EventEmitter = events.EventEmitter;

function DynamoBackup(options) {
  const params = {};
  options = options || {};
  this.excludedTables = options.excludedTables || [];
  this.includedTables = options.includedTables;
  this.readPercentage = options.readPercentage || 0.25;
  this.backupPath = options.backupPath;
  this.bucket = options.bucket;
  this.stopOnFailure = options.stopOnFailure || false;
  this.base64Binary = options.base64Binary || false;
  this.saveDataPipelineFormat = options.saveDataPipelineFormat || false;
  this.awsAccessKey = options.awsAccessKey;
  this.awsSecretKey = options.awsSecretKey;
  this.awsRegion = options.awsRegion;
  this.debug = Boolean(options.debug);

  if (this.awsRegion) {
    params.region = this.awsRegion;
  }
  if (this.awsAccessKey && this.awsSecretKey) {
    params.accessKeyId = this.awsAccessKey;
    params.secretAccessKey = this.awsSecretKey;
  }

  AWS.config.update(params);
}

util.inherits(DynamoBackup, EventEmitter);

DynamoBackup.prototype.listTables = function(callback) {
  const self = this;
  self._fetchTables(null, [], callback);
};

DynamoBackup.prototype.backupTable = function(tableName, backupPath, callback) {
  const self = this;
  const stream = new ReadableStream();

  if (_.isUndefined(callback)) {
    callback = backupPath;
    backupPath = self._getBackupPath();
  }

  const params = {};
  if (self.awsRegion) {
    params.region = self.awsRegion;
  }
  if (self.awsAccessKey && self.awsSecretKey) {
    params.accessKey = self.awsAccessKey;
    params.secretKey = self.awsSecretKey;
  }

  params.bucket = self.bucket;
  params.objectName = path.join(backupPath, tableName + '.json');
  params.stream = stream;
  params.debug = self.debug;

  const upload = new Uploader(params);

  const startTime = moment.utc();
  self.emit('start-backup', tableName, startTime);
  upload.send(function(err) {
    if (err) {
      self.emit('error', {
        table: tableName,
        err: err
      });
    }
    const endTime = moment.utc();
    const backupDuration = new dateRange(startTime, endTime); // eslint-disable-line new-cap
    self.emit('end-backup', tableName, backupDuration);
    return callback(err);
  });

  self._copyTable(
    tableName,
    function(items) {
      items.forEach(function(item) {
        if (self.base64Binary) {
          _.each(item, function(value) {
            if (value && value.B) {
              value.B = new Buffer(value.B).toString('base64');
            }
          });
        }

        if (self.saveDataPipelineFormat) {
          stream.append(self._formatForDataPipeline(item));
        } else {
          stream.append(JSON.stringify(item));
        }
        stream.append('\n');
      });
    },
    function(err) {
      stream.end();
      if (err) {
        self.emit('error', {
          table: tableName,
          err: err
        });
      }
    }
  );
};

DynamoBackup.prototype.backupAllTables = function(callback) {
  const self = this;
  const backupPath = self._getBackupPath();

  self.listTables(function(err, tables) {
    if (err) {
      callback(err);
    } else {
      const includedTables = self.includedTables || tables;
      tables = _.difference(tables, self.excludedTables);
      tables = _.intersection(tables, includedTables);

      async.each(tables,
        function(tableName, done) {
          self.backupTable(tableName, backupPath, function(err) { // eslint-disable-line no-shadow
            if (err) {
              if (self.stopOnFailure) {
                done(err);
                return;
              }
            }
            done();
          });
        },
        callback
      );
    }
  });
};

DynamoBackup.prototype._getBackupPath = function() {
  const self = this;
  const now = moment.utc();
  return self.backupPath || ('DynamoDB-backup-' + now.format('YYYY-MM-DD-HH-mm-ss'));
};

DynamoBackup.prototype._copyTable = function(tableName, itemsReceived, callback) {
  const self = this;
  const ddb = new AWS.DynamoDB();
  ddb.describeTable({
    TableName: tableName
  }, function(err, data) {
    if (err) {
      callback(err);
      return;
    }

    const readPercentage = self.readPercentage;
    const limit = Math.max((data.Table.ProvisionedThroughput.ReadCapacityUnits * readPercentage) | 0, 1);

    self._streamItems(tableName, null, limit, itemsReceived, callback);
  });
};

DynamoBackup.prototype._streamItems = function fetchItems(tableName, startKey, limit, itemsReceived, callback) {
  const self = this;
  const ddb = new AWS.DynamoDB();
  const params = {
    Limit: limit,
    ReturnConsumedCapacity: 'NONE',
    TableName: tableName
  };
  if (startKey) {
    params.ExclusiveStartKey = startKey;
  }
  ddb.scan(params, function(err, data) {
    if (err) {
      callback(err);
      return;
    }

    if (data.Items.length > 0) {
      itemsReceived(data.Items);
    }

    if (!data.LastEvaluatedKey || _.keys(data.LastEvaluatedKey).length === 0) {
      callback();
      return;
    }
    self._streamItems(tableName, data.LastEvaluatedKey, limit, itemsReceived, callback);
  });
};

DynamoBackup.prototype._fetchTables = function(lastTable, tables, callback) {
  const self = this;
  const ddb = new AWS.DynamoDB();
  const params = {};
  if (lastTable) {
    params.ExclusiveStartTableName = lastTable;
  }
  ddb.listTables(params, function(err, data) {
    if (err) {
      callback(err, null);
    } else {
      tables = tables.concat(data.TableNames);
      if (data.LastEvaluatedTableName) {
        self._fetchTables(data.LastEvaluatedTableName, tables, callback);
      } else {
        callback(null, tables);
      }
    }
  });
};

/**
 * AWS Data Pipeline import requires that each key in the Attribute list
 * be lower-cased and for sets start with a lower-case character followed
 * by an 'S'.
 *
 * Go through each attribute and create a new entry with the correct case
 */
DynamoBackup.prototype._formatForDataPipeline = function(item) {
  const self = this;
  _.each(item, function(value) {
    //value will be of the form: {S: 'xxx'}. Convert the key
    _.each(value, function(v, k) {
      const dataPipelineValueKey = self._getDataPipelineAttributeValueKey(k);
      value[dataPipelineValueKey] = v;
      value[k] = undefined; // eslint-disable-line no-undefined
      // for MAps and Lists, recurse until the elements are created with the correct case
      if (k === 'M' || k === 'L') {
        self._formatForDataPipeline(v);
      }
    });
  });
  return JSON.stringify(item);
};

DynamoBackup.prototype._getDataPipelineAttributeValueKey = function(type) {
  switch (type) {
    case 'S':
    case 'N':
    case 'B':
    case 'M':
    case 'L':
    case 'NULL':
      return type.toLowerCase();
    case 'BOOL':
      return 'bOOL';
    case 'SS':
      return 'sS';
    case 'NS':
      return 'nS';
    case 'BS':
      return 'bS';
    default:
      throw new Error('Unknown AttributeValue key: ' + type);
  }
};

module.exports = DynamoBackup;
