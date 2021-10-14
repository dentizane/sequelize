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
      user: config.username,
      flags: '-FOUND_ROWS',
      password: config.password,
      database: config.database,
      timezone: this.sequelize.options.timezone,
      typeCast: ConnectionManager._typecast.bind(this),
      bigNumberStrings: false,
      supportBigNumbers: true,
      ...config.dialectOptions
    };

    try {
      const connection = this.lib.odbcConnection(`DSN=${connectionConfig.database || config.DSN}`);

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
      && !connection._closing
      && !connection.stream.destroyed;
  }
}

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
