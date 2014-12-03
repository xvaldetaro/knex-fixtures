'use strict';

var _ = require('lodash');


function specCreates(regFixtures, diff) {
  return _.reduce(diff, function(diff, query) {
    if (query.query !== 'create') {
      diff.push(query);
      return diff;
    }

    var model = regFixtures[query.table].model;
    query.column = _.reduce(model, function(table, column, columnName) {
      table.push({
        name : columnName,
        type : matchType(regFixtures, column),
        typeModifiers : getModifiers(column)
      });

      return table;
    }, []);

    diff.push(query);
    return diff;
  }, []);
}

function getModifiers(column) {
  var modifiers = column.typeModifiers || '';

  if (column.reference) {
    var split = column.reference.split('.');
    modifiers += ' REFERENCES ' + split[0] + ' (' + split[1] + ')';
  }

  if (column.unique === true) {
    modifiers += ' UNIQUE ';
  }

  if (column.primary === true) {
    modifiers += ' PRIMARY KEY ';
  }

  return modifiers;
}

function getTypeForColumn (column) {
  switch (typeof column.method) {
    case 'number':
      if (Math.round(column.method) === column.method) {
        return 'integer';
      } else {
        return 'double precision';
      }
    case 'boolean':
      return 'boolean';
    case 'string':
      return matchFakerType(column.method, column.options);
    default:
      throw new Error('Value ' + column.method + ' is not supported without an explicit type annotation');
  }
}

function matchType (regFixtures, column) {

  if (column.type != null) {
    return column.type;
  } else if (column.method != null) {
    return getTypeForColumn(column);
  } else if (typeof column === 'string') {
    // Let's match a fakerjs method
    return matchFakerType(column);
  } else if (column.reference != null) {
    var split = column.reference.split('.');

    return matchType(regFixtures, regFixtures[split[0]].model[split[1]]);
  }
}

function matchFakerType(fnName, opts) {
  opts = opts || {};

  if (fnName === 'address.latitude' ||
      fnName === 'address.longitude' ||
      fnName === 'finance.amount') {
    return 'double precision';
  } else if (fnName.split('.')[0] === 'date') {
    return 'timestamp';
  } else if (fnName === 'random.array_element' ||
             fnName === 'random.object_element') {
    return getTypeForColumn({ method : _.values(opts)[0] });
  } else if (fnName === 'random.number') {
    if (opts.precision && opts.precision !== 1) {
      return 'double precision';
    } else {
      return 'integer';
    }
  } else {
    return 'varchar';
  }

}

module.exports = {
  specCreates : specCreates,
  matchType   : matchType,
  getModifiers: getModifiers
};
