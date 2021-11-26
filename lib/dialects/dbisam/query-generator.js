'use strict';

const _ = require('lodash');
const Utils = require('../../utils');
const AbstractQueryGenerator = require('../abstract/query-generator');
const util = require('util');
const Op = require('../../operators');


const JSON_FUNCTION_REGEX = /^\s*((?:[a-z]+_){0,2}jsonb?(?:_[a-z]+){0,2})\([^)]*\)/i;
const JSON_OPERATOR_REGEX = /^\s*(->>?|@>|<@|\?[|&]?|\|{2}|#-)/i;
const TOKEN_CAPTURE_REGEX = /^\s*((?:([`"'])(?:(?!\2).|\2{2})*\2)|[\w\d\s]+|[().,;+-])/i;

const typeWithoutDefault = new Set(['BLOB', 'TEXT', 'GEOMETRY', 'JSON']);

class DBISAMQueryGenerator extends AbstractQueryGenerator {
  constructor(options) {
    super(options);

    this.OperatorMap = {
      ...this.OperatorMap,
      [Op.regexp]: 'REGEXP',
      [Op.notRegexp]: 'NOT REGEXP',
      [Op.ne]: '<>'
    };
  }

  createDatabaseQuery(databaseName, options) {// eslint-disable-line no-unused-vars
    return 'start transaction; commit;';
  }

  dropDatabaseQuery(databaseName) {// eslint-disable-line no-unused-vars
    return 'start transaction; commit;';
  }

  createSchema() {
    return 'start transaction; commit;';
  }

  showSchemasQuery() {
    return 'start transaction; commit;';
  }

  versionQuery() {
    return 'start transaction; commit;';
  }

  createTableQuery(tableName, attributes, options) {
    options = {
      charset: null,
      rowFormat: null,
      ...options
    };

    const primaryKeys = [];
    const foreignKeys = {};
    const attrStr = [];
    const addIndexes = [];

    for (const attr in attributes) {
      if (!Object.prototype.hasOwnProperty.call(attributes, attr)) continue;
      const dataType = attributes[attr];
      // let match;

      if (dataType.includes('PRIMARY KEY')) {
        primaryKeys.push(attr);

        attrStr.push(`${this.quoteIdentifiers(attr)} ${dataType.replace('PRIMARY KEY', '')}`);
        // DBISAM doesn't support REFERENCES
        // if (dataType.includes('REFERENCES')) {
        //   match = dataType.match(/^(.+) (REFERENCES.*)$/);
        //   attrStr.push(`${this.quoteIdentifiers(attr)} ${match[1].replace('PRIMARY KEY', '')}`);
        //   foreignKeys[attr] = match[2];
        // } else {
        //   attrStr.push(`${this.quoteIdentifiers(attr)} ${dataType.replace('PRIMARY KEY', '')}`);
        // }
      } else if (dataType.includes('REFERENCES')) {
        // DBISAM doesn't support REFERENCES
        // match = dataType.match(/^(.+) (REFERENCES.*)$/);
        // attrStr.push(`${this.quoteIdentifiers(attr)} ${match[1]}`);
        // foreignKeys[attr] = match[2];
      } else {
        attrStr.push(`${this.quoteIdentifiers(attr)} ${dataType}`);
      }
    }

    const table = this.quoteTable(tableName);
    let attributesClause = attrStr.join(', ');
    const pkString = primaryKeys.map(pk => this.quoteIdentifiers(pk)).join(', ');

    if (options.uniqueKeys) {
      _.each(options.uniqueKeys, (columns, indexName) => {
        if (columns.customIndex) {
          if (typeof indexName !== 'string') {
            indexName = `uniq_${tableName}_${columns.fields.join('_')}`;
          }
          addIndexes.push(`CREATE UNIQUE INDEX IF NOT EXISTS ${this.quoteIdentifiers(indexName)} ON ${table} (${columns.fields.map(field => this.quoteIdentifiers(field)).join(', ')});`);
        } else {
          // TODO: Need something here?
        }
      });
    }

    if (pkString.length > 0) {
      attributesClause += `, PRIMARY KEY (${pkString})`;
    }

    for (const fkey in foreignKeys) {
      if (Object.prototype.hasOwnProperty.call(foreignKeys, fkey)) {
        attributesClause += `, FOREIGN KEY (${this.quoteIdentifiers(fkey)}) ${foreignKeys[fkey]}`;
      }
    }

    return Utils.joinSQLFragments([
      'CREATE TABLE IF NOT EXISTS',
      table,
      `(${attributesClause})`,
      options.comment && typeof options.comment === 'string' && `DESCRIPTION ${this.escape(options.comment)}`,
      options.initialAutoIncrement && `LAST AUTOINC=${options.initialAutoIncrement}`,
      ';',
      ...addIndexes
    ]);
  }

  describeTableQuery(tableName, schema, schemaDelimiter) {// eslint-disable-line no-unused-vars
    return 'start transaction; commit;';
  }

  showTablesQuery(database) {// eslint-disable-line no-unused-vars
    return 'start transaction; commit;';
  }

  addColumnQuery(table, key, dataType) {
    return Utils.joinSQLFragments([
      'ALTER TABLE',
      this.quoteTable(table),
      'ADD',
      this.quoteIdentifiers(key),
      this.attributeToSQL(dataType, {
        context: 'addColumn',
        tableName: table,
        foreignKey: key
      }),
      ';'
    ]);
  }

  removeColumnQuery(tableName, attributeName) {
    return Utils.joinSQLFragments([
      'ALTER TABLE',
      this.quoteTable(tableName),
      'DROP',
      this.quoteIdentifiers(attributeName),
      ';'
    ]);
  }

  changeColumnQuery(tableName, attributes) {
    const attrString = [];
    const constraintString = [];

    for (const attributeName in attributes) {
      let definition = attributes[attributeName];
      if (definition.includes('REFERENCES')) {
        const attrName = this.quoteIdentifiers(attributeName);
        definition = definition.replace(/.+?(?=REFERENCES)/, '');
        constraintString.push(`FOREIGN KEY (${attrName}) ${definition}`);
      } else {
        attrString.push(`"${attributeName}" "${attributeName}" ${definition}`);
      }
    }

    return Utils.joinSQLFragments([
      'ALTER TABLE',
      this.quoteTable(tableName),
      attrString.length && `REDEFINE ${attrString.join(', ')}`,
      constraintString.length && `ADD ${constraintString.join(', ')}`,
      ';'
    ]);
  }

  renameColumnQuery(tableName, attrBefore, attributes) {
    const attrString = [];

    for (const attrName in attributes) {
      const definition = attributes[attrName];
      attrString.push(`"${attrBefore}" "${attrName}" ${definition}`);
    }

    return Utils.joinSQLFragments([
      'ALTER TABLE',
      this.quoteTable(tableName),
      'REDEFINE',
      attrString.join(', '),
      ';'
    ]);
  }

  addLimitAndOffset(options) {
    let fragment = '';

    /* eslint-disable */
    if (options.limit != null) {
      fragment += ' TOP ' + this.escape(options.limit);
    }
    /* eslint-enable */

    return fragment;
  }

  handleSequelizeMethod(smth, tableName, factory, options, prepend) {
    if (smth instanceof Utils.Json) {
      // Parse nested object
      if (smth.conditions) {
        const conditions = this.parseConditionObject(smth.conditions).map(condition =>
          `${this.jsonPathExtractionQuery(condition.path[0], _.tail(condition.path))} = '${condition.value}'`
        );

        return conditions.join(' AND ');
      }
      if (smth.path) {
        let str;

        // Allow specifying conditions using the sqlite json functions
        if (this._checkValidJsonStatement(smth.path)) {
          str = smth.path;
        } else {
          // Also support json property accessors
          const paths = _.toPath(smth.path);
          const column = paths.shift();
          str = this.jsonPathExtractionQuery(column, paths);
        }

        if (smth.value) {
          str += util.format(' = %s', this.escape(smth.value));
        }

        return str;
      }
    } else if (smth instanceof Utils.Cast) {
      if (/timestamp/i.test(smth.type)) {
        smth.type = 'datetime';
      } else if (smth.json && /boolean/i.test(smth.type)) {
        // true or false cannot be casted as booleans within a JSON structure
        smth.type = 'char';
      } else if (/double precision/i.test(smth.type) || /boolean/i.test(smth.type) || /integer/i.test(smth.type)) {
        smth.type = 'decimal';
      } else if (/text/i.test(smth.type)) {
        smth.type = 'char';
      }
    }

    return super.handleSequelizeMethod(smth, tableName, factory, options, prepend);
  }

  _toJSONValue(value) {
    // true/false are stored as strings in DBISAM
    if (typeof value === 'boolean') {
      return value.toString();
    }
    // null is stored as a string in DBISAM
    if (value === null) {
      return 'null';
    }
    return value;
  }

  truncateTableQuery(tableName) {
    return `EMPTY TABLE ${this.quoteTable(tableName)}`;
  }

  deleteQuery(tableName, where, options = {}, model) {
    let limit = '';
    let query = `DELETE FROM ${this.quoteTable(tableName)}`;

    if (options.limit) {
      limit = ` TOP ${this.escape(options.limit)}`;
    }

    where = this.getWhereConditions(where, null, model, options);

    if (where) {
      query += ` WHERE ${where}`;
    }

    return query + limit;
  }

  showIndexesQuery(tableName, options) {// eslint-disable-line no-unused-vars
    return 'start transaction; commit;';
  }

  showConstraintsQuery(table, constraintName) {// eslint-disable-line no-unused-vars
    return 'start transaction; commit;';
  }

  removeIndexQuery(tableName, indexNameOrAttributes) {
    let indexName = indexNameOrAttributes;

    if (typeof indexName !== 'string') {
      indexName = Utils.underscore(`${tableName}_${indexNameOrAttributes.join('_')}`);
    }

    return Utils.joinSQLFragments([
      'DROP INDEX IF EXISTS',
      `${this.quoteTable(tableName)}.${this.quoteIdentifiers(indexName)}`
    ]);
  }

  attributeToSQL(attribute, options) {
    if (!_.isPlainObject(attribute)) {
      attribute = {
        type: attribute
      };
    }

    const attributeString = attribute.type.toString({ escape: this.escape.bind(this) });
    let template = attributeString;

    if (attribute.allowNull === false) {
      template += ' NOT NULL';
    }

    if (attribute.autoIncrement) {
      template = template.replace('INTEGER', 'AUTOINC');
    }

    // BLOB/TEXT/GEOMETRY/JSON cannot have a default value
    if (!typeWithoutDefault.has(attributeString)
      && attribute.type._binary !== true
      && Utils.defaultValueSchemable(attribute.defaultValue)) {
      template += ` DEFAULT ${this.escape(attribute.defaultValue)}`;
    }

    
    if (attribute.primaryKey) {
      template += ' PRIMARY KEY';
    }
    
    if (attribute.comment) {
      template += ` DESCRIPTION ${this.escape(attribute.comment)}`;
    }

    if (attribute.unique === true) {
      template += ' UNIQUE';
    }

    // if (attribute.first) {
    //   template += ' FIRST';
    // }
    // if (attribute.after) {
    //   template += ` AFTER ${this.quoteIdentifiers(attribute.after)}`;
    // }

    if (attribute.references) {
      if (options && options.context === 'addColumn' && options.foreignKey) {
        const attrName = this.quoteIdentifiers(options.foreignKey);
        const fkName = this.quoteIdentifiers(`${options.tableName}_${attrName}_foreign_idx`);

        template += `, ADD CONSTRAINT ${fkName} FOREIGN KEY (${attrName})`;
      }

      template += ` REFERENCES ${this.quoteTable(attribute.references.model)}`;

      if (attribute.references.key) {
        template += ` (${this.quoteIdentifiers(attribute.references.key)})`;
      } else {
        template += ` (${this.quoteIdentifiers('id')})`;
      }

      if (attribute.onDelete) {
        template += ` ON DELETE ${attribute.onDelete.toUpperCase()}`;
      }

      if (attribute.onUpdate) {
        template += ` ON UPDATE ${attribute.onUpdate.toUpperCase()}`;
      }
    }

    return template;
  }

  attributesToSQL(attributes, options) {
    const result = {};

    for (const key in attributes) {
      const attribute = attributes[key];
      result[attribute.field || key] = this.attributeToSQL(attribute, options);
    }

    return result;
  }

  /**
   * Check whether the statmement is json function or simple path
   *
   * @param   {string}  stmt  The statement to validate
   * @returns {boolean}       true if the given statement is json function
   * @throws  {Error}         throw if the statement looks like json function but has invalid token
   * @private
   */
  _checkValidJsonStatement(stmt) {
    if (typeof stmt !== 'string') {
      return false;
    }

    let currentIndex = 0;
    let openingBrackets = 0;
    let closingBrackets = 0;
    let hasJsonFunction = false;
    let hasInvalidToken = false;

    while (currentIndex < stmt.length) {
      const string = stmt.substr(currentIndex);
      const functionMatches = JSON_FUNCTION_REGEX.exec(string);
      if (functionMatches) {
        currentIndex += functionMatches[0].indexOf('(');
        hasJsonFunction = true;
        continue;
      }

      const operatorMatches = JSON_OPERATOR_REGEX.exec(string);
      if (operatorMatches) {
        currentIndex += operatorMatches[0].length;
        hasJsonFunction = true;
        continue;
      }

      const tokenMatches = TOKEN_CAPTURE_REGEX.exec(string);
      if (tokenMatches) {
        const capturedToken = tokenMatches[1];
        if (capturedToken === '(') {
          openingBrackets++;
        } else if (capturedToken === ')') {
          closingBrackets++;
        } else if (capturedToken === ';') {
          hasInvalidToken = true;
          break;
        }
        currentIndex += tokenMatches[0].length;
        continue;
      }

      break;
    }

    // Check invalid json statement
    if (hasJsonFunction && (hasInvalidToken || openingBrackets !== closingBrackets)) {
      throw new Error(`Invalid json statement: ${stmt}`);
    }

    // return true if the statement has valid json function
    return hasJsonFunction;
  }

  /**
   * Generates an SQL query that returns all foreign keys of a table.
   *
   * @param  {object} table  The table.
   * @param  {string} schemaName The name of the schema.
   * @returns {string}            The generated sql query.
   * @private
   */
  getForeignKeysQuery(table, schemaName) {// eslint-disable-line no-unused-vars
    return 'start transaction; commit;';
  }

  /**
   * Generates an SQL query that returns the foreign key constraint of a given column.
   *
   * @param  {object} table  The table.
   * @param  {string} columnName The name of the column.
   * @returns {string}            The generated sql query.
   * @private
   */
  getForeignKeyQuery(table, columnName) {// eslint-disable-line no-unused-vars
    return 'start transaction; commit;';
  }

  /**
   * Generates an SQL query that removes a foreign key from a table.
   *
   * @param  {string} tableName  The name of the table.
   * @param  {string} foreignKey The name of the foreign key constraint.
   * @returns {string}            The generated sql query.
   * @private
   */
  dropForeignKeyQuery(tableName, foreignKey) {// eslint-disable-line no-unused-vars
    return 'start transaction; commit;';
  }
}

module.exports = DBISAMQueryGenerator;
