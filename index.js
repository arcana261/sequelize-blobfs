"use strict";

const sequelizeMetaDb = require('sequelize-db-meta');
const task = require('xcane').task;
const type = require('xcane').type;

class SequelizeBlobFs {
  constructor(sequelize, name, blockSize, definitions, options) {
    this._db = new sequelizeMetaDb.MetaDB(
      sequelize, name, Object.assign({}, {
        data: {
          type: sequelize.Sequelize.BLOB
        }
      }));
    this._configDb = this._db.prefix('config:');
    this._nodeDb = this._db.prefix('node:');
    this._blobDb = this._db.prefix('blob:');
    this._blockSize = blockSize || 4096;
  }

  _targetDb(id) {
    return this._blobDb.prefix(`${id}:`);
  }

  _rawRead(id, blockSize, start, length, transaction) {
    const targetDb = this._targetDb(id);

    return task.spawn(function * task() {
      let block = Math.floor(start / blockSize);
      let offset = start % blockSize;
      let result = [Buffer.alloc(0)];

      while (length > 0) {
        const record = yield targetDb.getOrNull(`${block}`, transaction);

        if (type.isOptional(record)) {
          return Buffer.concat(result);
        }

        let toRead = Math.min(record.data.length - offset, length);
        result.push(record.data.slice(offset, offset + toRead));
        length -= toRead;
        offset = 0;
        block++;
      }

      return Buffer.concat(result);
    });
  }

  _rawWrite(id, blocksize, start, data, transaction) {
    const targetDb = this._targetDb(id);
  }
}
