'use strict';

const AbstractConnectionManager = require('../abstract/connection-manager');
const SequelizeErrors = require('../../errors');
const { logger } = require('../../utils/logger');
const DataTypes = require('../../data-types').dbisam;
const debug = logger.debugContext('connection:dbisam');
const parserStore = require('../parserStore')('dbisam');
const { promisify } = require('util');

/**
 * DBISAM Connection Manager
 *
 * Get connections, validate and disconnect them.
 * Use oledb to connect with DBISAM
 *
 * @private
 */
class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    sequelize.config.port = sequelize.config.port || 3306;
    super(dialect, sequelize);
    this.lib = this._loadDialectModule('oledb');
    this.refreshTypeParser(DataTypes);
  }

  _refreshTypeParser(dataType) {
    parserStore.refresh(dataType);
  }

  _clearTypeParser() {
    parserStore.clear();
  }

  static _typecast(field, next) {
    if (parserStore.get(field.type)) {
      return parserStore.get(field.type)(field, this.sequelize.options, next);
    }
    return next();
  }

  /**
   * Set up DBISAM ODBC connection
   *
   * @param {object} config
   * @returns {Promise<Connection>}
   * @private
   */
  async connect(config) {
    const connectionConfig = {
      host: config.host,
      port: config.port,
      user: config.username || 'admin',
      flags: '-FOUND_ROWS',
      password: config.password || 'DBAdmin',
      database: config.database,
      connType: config.connType || 'LOCAL',
      DSN: config.DSN,
      timezone: this.sequelize.options.timezone,
      typeCast: ConnectionManager._typecast.bind(this),
      bigNumberStrings: false,
      supportBigNumbers: true,
      ...config.dialectOptions
    };

    try {
      const connection = this.lib.odbcConnection(this.buildConnString(connectionConfig));

      debug('connection acquired');

      return connection;
    } catch (err) {
      switch (err.code) {
        case 'ECONNREFUSED':
          throw new SequelizeErrors.ConnectionRefusedError(err);
        case 'ER_ACCESS_DENIED_ERROR':
          throw new SequelizeErrors.AccessDeniedError(err);
        case 'ENOTFOUND':
          throw new SequelizeErrors.HostNotFoundError(err);
        case 'EHOSTUNREACH':
          throw new SequelizeErrors.HostNotReachableError(err);
        case 'EINVAL':
          throw new SequelizeErrors.InvalidConnectionError(err);
        default:
          throw new SequelizeErrors.ConnectionError(err);
      }
    }
  }

  buildConnString(config) {
    let connStr = '';
    if (config.DSN) {
      connStr = `DSN=${config.DSN}`;
    } else {
      connStr += `DRIVER={DBISAM 4 ODBC Driver};CatalogName=${config.database};ConnectionType=${config.connType};`;
      
      if (config.connType === 'REMOTE') {
        if (config.host) {
          connStr += /([0-9]{3}\.){3}[0-9]{3}/.test(config.host) ? 'RemoteIPAddress=' : 'RemoteHostName=';
          connStr += `${config.host};`;
        }
        connStr += config.user ? `UID=${config.user};` : '';
        connStr += config.password ? `PWD=${config.password};` : '';
        connStr += config.port ? `RemotePort=${config.port};` : '';
      } else {
        const err = new Error();
        err.code = 'EINVAL';
        throw err;
      }
    }

    if (config.pwds) {
      for (const [i, pwd] of config.pwds.entries()) {
        connStr += `TablePassword${i}=${pwd};`;
      }
    }

    return connStr;
  }

  async disconnect(connection) {
    // Don't disconnect connections with CLOSED state
    if (connection._closing) {
      debug('connection tried to disconnect but was already at CLOSED state');
      return;
    }

    return await promisify(callback => callback())();
  }

  validate(connection) {
    return connection
      && !connection._fatalError
      && !connection._protocolError
      && !connection._closing;
  }
}

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
