'use strict';

const wkx = require('wkx');
const _ = require('lodash');
const moment = require('moment-timezone');
module.exports = BaseTypes => {
  BaseTypes.ABSTRACT.prototype.dialectTypes = 'https://dev.dbisam.com/doc/refman/5.7/en/data-types.html';

  /**
   * types: [buffer_type, ...]
   *
   * @see buffer_type here https://dev.dbisam.com/doc/refman/5.7/en/c-api-prepared-statement-type-codes.html
   * @see hex here https://github.com/sidorares/node-dbisam2/blob/master/lib/constants/types.js
   */

  BaseTypes.DATE.types.dbisam = ['DATETIME'];
  BaseTypes.STRING.types.dbisam = ['VAR_STRING'];
  BaseTypes.CHAR.types.dbisam = ['STRING'];
  BaseTypes.TEXT.types.dbisam = ['BLOB'];
  BaseTypes.TINYINT.types.dbisam = ['TINY'];
  BaseTypes.SMALLINT.types.dbisam = ['SHORT'];
  BaseTypes.MEDIUMINT.types.dbisam = ['INT24'];
  BaseTypes.INTEGER.types.dbisam = ['LONG'];
  BaseTypes.BIGINT.types.dbisam = ['LONGLONG'];
  BaseTypes.FLOAT.types.dbisam = ['FLOAT'];
  BaseTypes.TIME.types.dbisam = ['TIME'];
  BaseTypes.DATEONLY.types.dbisam = ['DATE'];
  BaseTypes.BOOLEAN.types.dbisam = ['TINY'];
  BaseTypes.BLOB.types.dbisam = ['TINYBLOB', 'BLOB', 'LONGBLOB'];
  BaseTypes.DECIMAL.types.dbisam = ['NEWDECIMAL'];
  BaseTypes.UUID.types.dbisam = false;
  BaseTypes.ENUM.types.dbisam = false;
  BaseTypes.REAL.types.dbisam = ['DOUBLE'];
  BaseTypes.DOUBLE.types.dbisam = ['DOUBLE'];
  BaseTypes.GEOMETRY.types.dbisam = ['GEOMETRY'];
  BaseTypes.JSON.types.dbisam = ['JSON'];

  class DECIMAL extends BaseTypes.DECIMAL {
    toSql() {
      let definition = super.toSql();
      if (this._unsigned) {
        definition += ' UNSIGNED';
      }
      if (this._zerofill) {
        definition += ' ZEROFILL';
      }
      return definition;
    }
  }

  class DATE extends BaseTypes.DATE {
    toSql() {
      return this._length ? `DATETIME(${this._length})` : 'DATETIME';
    }
    _stringify(date, options) {
      date = this._applyTimezone(date, options);
      
      if (this._length) {
        return date.format('YYYY-MM-DD HH:mm:ss.SSS');
      }
      return date.format('YYYY-MM-DD HH:mm:ss');
    }
    static parse(value, options) {
      value = value.string();
      if (value === null) {
        return value;
      }
      if (moment.tz.zone(options.timezone)) {
        value = moment.tz(value, options.timezone).toDate();
      }
      else {
        value = new Date(`${value} ${options.timezone}`);
      }
      return value;
    }
  }

  class DATEONLY extends BaseTypes.DATEONLY {
    static parse(value) {
      return value.string();
    }
  }
  class UUID extends BaseTypes.UUID {
    toSql() {
      return 'CHAR(36) BINARY';
    }
  }

  const SUPPORTED_GEOMETRY_TYPES = ['POINT', 'LINESTRING', 'POLYGON'];

  class GEOMETRY extends BaseTypes.GEOMETRY {
    constructor(type, srid) {
      super(type, srid);
      if (_.isEmpty(this.type)) {
        this.sqlType = this.key;
        return;
      }
      if (SUPPORTED_GEOMETRY_TYPES.includes(this.type)) {
        this.sqlType = this.type;
        return;
      }
      throw new Error(`Supported geometry types are: ${SUPPORTED_GEOMETRY_TYPES.join(', ')}`);
    }
    static parse(value) {
      value = value.buffer();
      // Empty buffer, dbisam doesn't support POINT EMPTY
      // check, https://dev.dbisam.com/worklog/task/?id=2381
      if (!value || value.length === 0) {
        return null;
      }
      // For some reason, discard the first 4 bytes
      value = value.slice(4);
      return wkx.Geometry.parse(value).toGeoJSON({ shortCrs: true });
    }
    toSql() {
      return this.sqlType;
    }
  }

  class ENUM extends BaseTypes.ENUM {
    toSql(options) {
      return `ENUM(${this.values.map(value => options.escape(value)).join(', ')})`;
    }
  }

  class JSONTYPE extends BaseTypes.JSON {
    _stringify(value, options) {
      return options.operation === 'where' && typeof value === 'string' ? value : JSON.stringify(value);
    }
  }

  return {
    ENUM,
    DATE,
    DATEONLY,
    UUID,
    GEOMETRY,
    DECIMAL,
    JSON: JSONTYPE
  };
};
