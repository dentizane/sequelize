'use strict';

const _ = require('lodash');
const AbstractDialect = require('../abstract');
const ConnectionManager = require('./connection-manager');
const Query = require('./query');
const QueryGenerator = require('./query-generator');
const DataTypes = require('../../data-types').dbisam;
const { DBISAMQueryInterface } = require('./query-interface');

class DBISAMDialect extends AbstractDialect {
  constructor(sequelize) {
    super();
    this.sequelize = sequelize;
    this.connectionManager = new ConnectionManager(this, sequelize);
    this.queryGenerator = new QueryGenerator({
      _dialect: this,
      sequelize
    });
    this.queryInterface = new DBISAMQueryInterface(sequelize, this.queryGenerator);
  }
}

DBISAMDialect.prototype.supports = _.merge(_.cloneDeep(AbstractDialect.prototype.supports), {
  'VALUES ()': true,
  'LIMIT ON UPDATE': false,
  lock: true,
  forShare: 'LOCK IN SHARE MODE',
  settingIsolationLevelDuringTransaction: false,
  // inserts: {
  //   ignoreDuplicates: ' IGNORE',
  //   updateOnDuplicate: ' ON DUPLICATE KEY UPDATE'
  // },
  autoIncrement: {
    identityInsert: false,
    defaultValue: false,
    update: false
  },
  index: {
    collate: false,
    length: false,
    parser: false,
    type: true,
    using: 0
  },
  constraints: {
    dropConstraint: false,
    check: false
  },
  schemas: true,
  indexViaAlter: false,
  indexHints: true,
  NUMERIC: true,
  GEOMETRY: false,
  JSON: false,
  REGEXP: false
});

DBISAMDialect.prototype.defaultVersion = '4.48.0';
DBISAMDialect.prototype.Query = Query;
DBISAMDialect.prototype.QueryGenerator = QueryGenerator;
DBISAMDialect.prototype.DataTypes = DataTypes;
DBISAMDialect.prototype.name = 'dbisam';
DBISAMDialect.prototype.TICK_CHAR = '"';
DBISAMDialect.prototype.TICK_CHAR_LEFT = DBISAMDialect.prototype.TICK_CHAR;
DBISAMDialect.prototype.TICK_CHAR_RIGHT = DBISAMDialect.prototype.TICK_CHAR;

module.exports = DBISAMDialect;
