"use strict";

const expect = require('chai').expect;
const task = require('xcane').task;
const Sequelize = require('sequelize');
const BlobFs = require('../index');

describe('sequelize-db-meta', () => {
  let sequelize = null;
  let x = null;

  before(done => {
    sequelize = new Sequelize({
      dialect: 'sqlite',
      storage: ':memory:',
      benchmark: true
    });

    x = new BlobFs(sequelize);

    sequelize.sync({
      logging: console.log
    }).then(() => done()).catch(done);
  });

  describe('#x', () => {
    it('isiisdisd', () => task.spawn(function* () {
      yield x._createNode('0', '/', 'folder', null);
      yield x._createNode('123', 'a', 'file', '0');
    }));
  });
});
