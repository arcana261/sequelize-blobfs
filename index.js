"use strict";

const sequelizeMetaDb = require('sequelize-db-meta');
const task = require('xcane').task;
const type = require('xcane').type;

const _Type = Object.freeze({
  DIRECTORY: 'directory',
  FILE: 'file'
});

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
    });
  }

  _hasBlob() {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();

      if (type.isNull(self._blob)) {
        const totalBlocks = Math.ceil(self._size / self._blockSize);
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
          self._blob = (yield self._blobDb.get(`${self._block}`)).data;
          self._dirtyBlob = false;
        } else {
          self._blob = new Buffer(self._blockSize);
          self._dirtyBlob = true;
        }
      }

      return self._blob;
    });
  }

  _setPosition(pos) {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();

      if (pos > self._size) {
        throw new Error(`New position ${pos} exceeds size ${self._size}`);
      }

      const newBlock = Math.floor(pos / self._blockSize);

      if (newBlock !== self._block) {
        yield self._unloadBlob();
      }

      self._block = newBlock;
      self._index = pos % self._blockSize;
      self._pos = pos;
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
      const result = blob[self._index++];
      self._pos++;

      if (self._index >= self._blockSize) {
        yield self._unloadBlob();
        self._index = 0;
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

      blob[self._index++] = value;
      self._dirtyBlob = true;
      self._pos++;

      if (self._index >= self._blockSize) {
        yield self._unloadBlob();
        self._index = 0;
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

      if (type.isOptional(offset)) {
        offset = 0;
      }

      if (type.isOptional(length)) {
        length = buffer.length - offset;
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

      if (type.isOptional(offset)) {
        offset = 0;
      }

      if (type.isOptional(length)) {
        length = buffer.length - offset;
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
  }

  close() {
    const self = this;

    return task.spawn(function * task() {
      yield self._unloadBlob();

      if (self._dirtyHeader) {
        yield self._nodeDb.assign(`${this._id}`, {
          size: self._size
        });
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
      }, definitions), options);
    this._configDb = this._db.prefix('config:');
    this._nodeDb = this._db.prefix('node:');
    this._masterBlobDb = this._db.prefix('blob:');
    this._blockSize = blockSize || 4096;
  }

  _blobDb(id) {
    return this._masterBlobDb.prefix(`${id}:`);
  }

  _createEmptyDirectory(id, parent) {
    const self = this;

    return task.spawn(function * task() {
      
    });
  }
}
