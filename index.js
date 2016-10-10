"use strict";

const sequelizeMetaDb = require('sequelize-db-meta');
const task = require('xcane').task;
const type = require('xcane').type;

const _Type = Object.freeze({
  DIRECTORY: 'directory',
  FILE: 'file'
});

class _SequelizeBlobFsCursor {
  constructor(nodeDb, blobDb, id) {
    this._nodeDb = nodeDb;
    this._blobDb = blobDb;
    this._id = id;
    this._block = 0;
    this._index = 0;
    this._pos = 0;
    this._size = null;
    this._blockSize = null;
    this._name = null;
    this._type = null;
    this._parent = null;
    this._creation = null;
    this._access = null;
    this._blob = null;
    this._dirtyBlob = false;
    this._dirtyHeader = false;
  }

  open() {
    const self = this;

    return task.spawn(function * task() {
      const find = yield self._nodeDb.all(0, 1, `${this._id}`);

      if (find.length < 1) {
        throw new Error(`file not found`);
      }

      const meta = find[0];
      self._size = meta.size;
      self._blockSize = meta.value.blockSize;
      self._name = meta.name;
      self._type = meta.type;
      self._parent = meta.parent;
      self._creation = meta.creation;
      self._access = meta.access;
    });
  }

  _unloadBlob() {
    const self = this;

    return task.spawn(function * task() {
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
      if (type.isOptional(self._blob)) {
        const totalBlocks = Math.ceil(self._size / self._blockSize);
        return this._block < totalBlocks;
      }

      return true;
    });
  }

  _loadBlob() {
    const self = this;

    return task.spawn(function * task() {
      if (type.isOptional(self._blob)) {
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

      let bytesRead = 0;
      while (length > 0) {
        if (self._pos >= self._size) {
          break;
        }

        if (self._index >= self._blockSize) {
          yield self._unloadBlob();
          self._block++;
          self._index = 0;
        }

        const canRead = Math.min(self._blockSize - self._index,
           self._size - self._pos, length);
        const blob = yield self._loadBlob();

        for (let i = 0; i < canRead; i++) {
          buffer[offset + i] = blob[self._index + i];
        }

        offset += canRead;
        self._index += canRead;
        bytesRead += canRead;
        self._pos += canRead;
        length -= canRead;
      }

      return bytesRead;
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

      let bytesWritten = 0;
      while (length > 0) {
        if (self._pos >= self._size) {
          self._size++;
          self._dirtyHeader = true;
        }

        if (self._index >= self._blockSize) {
          yield self._unloadBlob();
          self._block++;
          self._index = 0;
        }

        const canWrite = Math.min(self._blockSize - self._index, length);
        const blob = self._loadBlob();

        for (let i = 0; i < canWrite; i++) {
          blob[self._index + i] = buffer[offset + i];
        }

        offset += canWrite;
        self._index += canWrite;
        bytesWritten += canWrite;
        self._pos += canWrite;
        length -= canWrite;
        self._dirtyBlob = true;
      }

      return bytesWritten;
    });
  }

  size() {
    return Promise.resolve(this._size);
  }

  eof() {
    return Promise.resolve(this._pos >= this._size);
  }

  empty() {
    return Promise.resolve(this._size < 1);
  }

  name() {
    return Promise.resolve(this._name);
  }

  creation() {
    return Promise.resolve(this._creation);
  }

  access() {
    return Promise.resolve(this._access);
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
    if (type.isOptional(name)) {
      name = '__blobfs';
    }

    let nodeOptions = Object.assign({}, options);
    let blobOptions = Object.assign({}, options);

    nodeOptions.indexes = (nodeOptions.indexes || []).concat([{
      name: 'name_index',
      method: 'btree',
      fields: [{
        attribute: 'name'
      }]
    }]);

    this._nodeDb = new sequelizeMetaDb.MetaDB(
      sequelize, name, Object.assign({
        name: {
          type: sequelize.Sequelize.TEXT,
          allowNull: false
        },
        size: {
          type: sequelize.Sequelize.BIGINT,
          allowNull: false
        },
        type: {
          type: sequelize.Sequelize.TEXT,
          allowNull: false
        },
        creation: {
          type: sequelize.Sequelize.DATE,
          allowNull: false
        },
        access: {
          type: sequelize.Sequelize.DATE,
          allowNull: false
        }
      }, definitions), nodeOptions);
    this._blobDb = new sequelizeMetaDb.MetaDB(
      sequelize, `${name}_blobs`, {
        data: {
          type: sequelize.Sequelize.BLOB
        }
      }, Object.assign({}, blobOptions, {
        expires: false
      }));

    this._nodeDb.schema.hasMany(this._blobDb.schema, {
      onDelete: 'CASCADE'
    });
    this._blobDb.schema.belongsTo(this._nodeDb.schema);
    this._nodeDb.schema.hasMany(this._nodeDb.schema, {
      as: 'parent',
      onDelete: 'CASCADE'
    });

    this._sequelize = sequelize;
    this._name = name;
    this._blockSize = blockSize || 4096;
  }

  _blobDb(id) {
    return this._masterBlobDb.prefix(`${id}:`);
  }

  _createNode(id, name, nodeType, parent) {
    const self = this;

    return task.spawn(function * task() {
      yield self._nodeDb.put(`${id}`, {
        blockSize: self._blockSize
      }, {
        name: name,
        type: nodeType,
        size: 0,
        creation: new Date(),
        access: new Date()
      });

      if (!type.isOptional(parent)) {
        const record = yield self._nodeDb.all(0, 1, `${id}`);
        const parentRecord = yield self._nodeDb.all(0, 1, `${parent}`);

        if (record.length < 1) {
          throw new Error(`internal error while finding record ${id}`);
        }

        if (parentRecord.length < 1) {
          throw new Error(`parent node not found: ${parent}`);
        }

        yield parentRecord[0].setParent(record[0]);
      }
    });
  }

  _createEmptyFile(id, name, parent) {
    return this._createNode(id, name, _Type.FILE, parent);
  }

  _createEmptyDirectory(id, name, parent) {
    return this._createNode(id, name, _Type.DIRECTORY, parent);
  }
}

module.exports = SequelizeBlobFs;
