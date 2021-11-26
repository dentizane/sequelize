'use strict';

const moment = require('moment-timezone');
module.exports = BaseTypes => {
  const warn = BaseTypes.ABSTRACT.warn.bind(undefined, 'http://www.postgresql.org/docs/9.4/static/datatype.html');
  BaseTypes.ABSTRACT.prototype.dialectTypes = 'https://dev.dbisam.com/doc/refman/5.7/en/data-types.html';

  /**
   * types: [buffer_type, ...]
   * https://www.elevatesoft.com/manual?action=viewtopic&id=dbisam4&product=delphi&version=7&topic=data_types_null_support
   * 
   */

  BaseTypes.DATE.types.dbisam = ['TIMESTAMP'];
  BaseTypes.STRING.types.dbisam = ['VARCHAR'];
  BaseTypes.CHAR.types.dbisam = ['CHAR'];
  BaseTypes.TEXT.types.dbisam = ['MEMO'];
  BaseTypes.TINYINT.types.dbisam = ['SMALLINT'];
  BaseTypes.SMALLINT.types.dbisam = ['SMALLINT'];
  BaseTypes.MEDIUMINT.types.dbisam = ['INTEGER'];
  BaseTypes.INTEGER.types.dbisam = ['INTEGER'];
  BaseTypes.BIGINT.types.dbisam = ['LARGEINT'];
  BaseTypes.FLOAT.types.dbisam = ['FLOAT'];
  BaseTypes.TIME.types.dbisam = ['TIME'];
  BaseTypes.DATEONLY.types.dbisam = ['DATE'];
  BaseTypes.BOOLEAN.types.dbisam = ['BOOLEAN'];
  BaseTypes.BLOB.types.dbisam = ['BLOB'];
  BaseTypes.DECIMAL.types.dbisam = ['BCD'];
  BaseTypes.UUID.types.dbisam = ['GUID'];
  BaseTypes.ENUM.types.dbisam = false;
  BaseTypes.REAL.types.dbisam = ['FLOAT'];
  BaseTypes.DOUBLE.types.dbisam = ['FLOAT'];
  BaseTypes.GEOMETRY.types.dbisam = false;
  BaseTypes.JSON.types.dbisam = false;

  function removeUnsupportedOptions(dataType) {
    if (dataType._length || dataType.options.length || dataType._unsigned || dataType._zerofill) {
      warn(`DBISAM does not support LENGTH, UNSIGNED, PRECISION or ZEROFILL. Plain '${dataType.key}' will be used instead.`);
      dataType._length = undefined;
      dataType.options.length = undefined;
      dataType._unsigned = undefined;
      dataType._zerofill = undefined;
    }
  }

  class DATE extends BaseTypes.DATE {
    toSql() {
      return 'TIMESTAMP';
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

  class TEXT extends BaseTypes.TEXT {
    toSql() {
      return 'MEMO';
    }
  }

  class NUMBER extends BaseTypes.NUMBER {
    toSql() {
      return this.key;
    }
  }

  class INTEGER extends BaseTypes.INTEGER {
    toSql() {
      return this.key;
    }
  }

  class SMALLINT extends BaseTypes.SMALLINT {
    toSql() {
      return this._unsigned ? 'WORD' : this.key;
    }
  }

  class BIGINT extends BaseTypes.BIGINT {
    toSql() {
      return 'LARGEINT';
    }
  }

  class FLOAT extends BaseTypes.FLOAT {
    constructor(length) {
      super(length);
      removeUnsupportedOptions(this);
    }

    toSql() {
      return 'FLOAT';
    }
  }

  class DOUBLE extends BaseTypes.DOUBLE {
    toSql() {
      return 'FLOAT';
    }
  }

  class DECIMAL extends BaseTypes.DECIMAL {
    toSql() {
      return 'BCD';
    }
  }

  class BOOLEAN extends BaseTypes.BOOLEAN {
    toSql() {
      return 'BOOLEAN';
    }
  }

  class UUID extends BaseTypes.UUID {
    toSql() {
      return 'GUID';
    }
  }
  
  class TIME extends BaseTypes.TIME {
    toSql() {
      return 'TIME';
    }
  }
  
  class BLOB extends BaseTypes.BLOB {
    toSql() {
      return 'BLOB';
    }
  }

  class NOW extends BaseTypes.NOW {
    toSql() {
      return 'CURRENT_TIME';
    }
  }

  return {
    DATE,
    DATEONLY,
    TEXT,
    NUMBER,
    INTEGER,
    SMALLINT,
    TINYINT: SMALLINT,
    MEDIUMINT: INTEGER,
    BIGINT,
    FLOAT,
    REAL: FLOAT,
    DOUBLE,
    'DOUBLE PRECISION': DOUBLE,
    DECIMAL,
    BOOLEAN,
    UUID,
    UUIDV1: UUID,
    UUIDV4: UUID,
    TIME,
    BLOB,
    NOW
  };
};
