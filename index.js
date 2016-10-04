"use strict";

const sequelizeMetaDb = require('sequelize-db-meta');
const task = require('xcane').task;
const type = require('xcane').type;

class SequelizeBlobFsCursor {
  constructor(nodeDb, blobDb, id) {
    this._nodeDb = nodeDb;
    this._blobDb = blobDb;
    this._id = id;
    this._block = 0;
    this._index = 0;
    this._pos = 0;
    this._size = null;
    this._blockSize = null;
    this._blob = null;
    this._dirtyBlob = false;
    this._dirtyHeader = false;
  }

  _ensure() {
    if (type.isNull(this._size) || type.isNull(this._blockSize)) {
      return this._nodeDb.get(`${this._id}`).then(meta => {
        this._size = meta.size;
        this._blockSize = meta.blockSize;

        return Promise.resolve();
      });
    }

    return Promise.resolve();
  }

  _setPosition(pos) {
    this._ensure().then(() => {
      if (pos > this._size) {
        return Promise.reject(new Error(
          `New position ${pos} exceeds size ${this._size}`));
      }

      this._block = Math.floor(pos / this._blockSize);
      this._index = pos % this._blockSize;
      this._pos = pos;

      return Promise.resolve();
    });
  }

  _unloadBlob() {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();

      if (self._dirtyBlob) {
        yield self._blobDb.put(`${self._block}`, null, {
          data: self._blob
        });
      }

      self._dirtyBlob = false;
      self._blob = null;
      self._index = 0;
    });
  }

  _hasBlob() {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();

      if (type.isNull(self._blob)) {
        const totalBlocks = Math.floor(self._size / self._blockSize);
        return this._block < totalBlocks;
      }

      return true;
    });
  }

  _loadBlob() {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();

      if (type.isNull(self._blob)) {
        if (yield self._hasBlob()) {
          const result = (yield self._blobDb.get(`${self._block}`)).data;
          self._index = 0;
          self._blob = result;
          self._dirtyBlob = false;
          return result;
        }

        const result = new Buffer(self._blockSize);
        self._index = 0;
        self._blob = result;
        self._dirtyBlob = true;
        return result;
      }

      return self._blob;
    });
  }

  _readByte() {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();

      if (self._pos === self._size) {
        throw new Error(
          `Position ${self._pos} exceeds size ${self._size}`);
      }

      const blob = yield self._loadBlob();
      const result = blob[self._index];
      self._index++;
      self._pos++;

      if (self._index >= self._blockSize) {
        yield self._unloadBlob();
        self._block++;
      }

      return result;
    });
  }

  _writeByte(value) {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();
      const blob = yield self._loadBlob();

      if (self._pos === self._size) {
        self._size++;
        self._dirtyHeader = true;
      }

      blob[self._index] = value;
      self._dirtyBlob = true;
      self._index++;
      self._pos++;

      if (self._index >= self._blockSize) {
        yield self._unloadBlob();
        self._block++;
      }
    });
  }

  read(buffer, offset, length, position) {
    const self = this;

    return task.spawn(function * task() {
      if (!type.isOptional(position)) {
        yield self._setPosition(position);
      }

      for (let i = 0; i < length; i++) {
        buffer[offset + i] = yield self._readByte();
      }
    });
  }

  write(buffer, offset, length, position) {
    const self = this;

    return task.spawn(function * task() {
      if (!type.isOptional(position)) {
        yield self._setPosition(position);
      }

      for (let i = 0; i < length; i++) {
        yield self._writeByte(buffer[offset + i]);
      }
    });
  }

  size() {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();
      return self._size;
    });


  close() {
    const self = this;

    return task.spawn(function * task() {
      self._unloadBlob();

      if (self._dirtyHeader) {

      }
    });
  }
}

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

  _rawWrite(id, blockSize, start, length, data, transaction) {
    const targetDb = this._targetDb(id);

    return task.spawn(function * task() {
      let block = Math.floor(start / blockSize);
      let offset = start % blockSize;
    });
  }
}
