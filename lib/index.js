'use strict';

var topsort        = require('topsort');
var _              = require('lodash');
var fixtureFactory = require('fixture-factory');
var Promise        = require('bluebird');

var utils = require('./utils');


var regFixtures   = {};
var defaults   = {
  howMany: 500
};
var knexInstance;

function unregisterFixtures() {
  _.forEach(regFixtures, function(fixture) {
    fixtureFactory.unregister(fixture.name);
  });
}

function init(fixtureSchema, knex) {
  _.forEach(fixtureSchema, function(fixture) {
    regFixtures[fixture.name] = fixture;
  });

  knexInstance = knex;
}

function upgrade(options, knex) {
  options = _.assign({}, defaults, options);
  knex = knex || knexInstance;

  var edges = _.reduce(regFixtures, function(edges, fixture) {
    if (fixture.depends) {
      _.forEach(fixture.depends, function(depends) {
        edges.push([ depends , fixture.name ]);
      });
    } else {
      edges.push([ fixture.name ]);
    }

    return edges;
  }, []);

  var chain = Promise.resolve();

  return Promise.map(topsort(edges), function(fixtureName) {
    var fixture = regFixtures[fixtureName];

    if (!fixtureFactory.dataModels[fixtureName]) {
      fixtureFactory.register(fixtureName, fixture.model);
    }

    var fixtures = fixtureFactory.generate(fixtureName, options.howMany);

    return knex(fixtureName).insert(fixtures);
  }, { concurrency : 1 });
}

function downgrade(options, knex) {
  options = _.assign({}, defaults, options);
  knex = knex || knexInstance;

  var edges = _.reduce(regFixtures, function(edges, fixture) {
    if (fixture.depends) {
      _.forEach(fixture.depends, function(depends) {
        edges.push([ fixture.name, depends ]);
      });
    } else {
      edges.push([ fixture.name ]);
    }

    return edges;
  }, []);

  var deleteOpts = Promise.map(topsort(edges), function(fixture) {
    return knex(fixture).del();
  }, { concurrency : 1 });

  unregisterFixtures();
  return deleteOpts;
}

function genDiff(options, knex) {
  options = _.assign({}, defaults, options);
  knex = knex || knexInstance;

  var edges = _.reduce(regFixtures, function(edges, fixture) {
    if (fixture.depends) {
      _.forEach(fixture.depends, function(depends) {
        edges.push([ depends , fixture.name ]);
      });
    } else {
      edges.push([ fixture.name ]);
    }

    return edges;
  }, []);

  var orderedTables = topsort(edges);

  knex.select().table('pg_catalog.pg_tables').then(function(tables) {
    var diff = [];

    var dbTables = _.chain(tables)
      .filter(function(table) {
        return table.schemaname === 'public';
      })
      .map(function(table) {
        return table.tablename;
      })
      .value();

    _.forEach(orderedTables, function(modelTable) {
      if (!_.contains(dbTables, modelTable)) {
        diff.push({ query : 'create', table : modelTable });
      }
    });

    diff = utils.specCreates(regFixtures, diff);

    _.forEach(dbTables, function(dbTable) {
      if (!_.contains(orderedTables, dbTable)) {
        diff.push({ query : 'drop', table : dbTable });
      }
    });

    return findAlterations(knex, diff, orderedTables, dbTables)
  })
  .then(function(diff) {
    console.log(JSON.stringify(diff, undefined, 2));
    process.exit();
  });

}

function findAlterations(knex, diff, modelTables, dbTables) {
  return knex.select().table('information_schema.columns').then(function(tableDef) {
    var dropOrCreateNames = _.pluck(diff, 'table');

    var alterableTables = _.reject(modelTables, function(table) {
      return _.contains(dropOrCreateNames, table);
    });

    _.forEach(alterableTables, function(modelTable) {
      var currentSchema = _.chain(tableDef)
      .filter(function(def) {
        return def.table_name === modelTable;
      })
      .reduce(function(schema, column) {
        schema[column.column_name] = column;
        return schema;
      }, {})
      .value();

      var tableModel = regFixtures[modelTable].model;
      var tableDiff = _.reduce(tableModel, function(diff, column, columnName) {
        if (columnName[0] === '_') {
          // We don't want to look for changes in stuff that is hidden,
          // and probably not even a column
          return;
        }

        var modelType = eigen(utils.matchType(regFixtures, column));
        var dbType = eigen(currentSchema[columnName].data_type);
        var userType = eigen(currentSchema[columnName].udt_name);

        if ((dbType !== 'USER-DEFINED' && modelType !== dbType) ||
            (dbType === 'USER-DEFINED' && modelType !== userType)) {

          // There is a difference, recreate the table
          // DROPs need to be CASCADE
          diff.push({ operation : 'drop', column : columnName });
          diff.push({
            operation : 'create',
            column : columnName,
            type : modelType,
            typeModifiers : utils.getModifiers(column)
          });
        }

        return diff;
      }, []);

      if (tableDiff.length > 0) {
        diff.push({
          query : 'alter',
          table : modelTable,
          diff : tableDiff
        });
      }
    });

    return Promise.resolve(diff);
  });
}

function eigen(type) {
  // Returns an eigentype for a given type
  if (type === 'varchar') {
    return 'character varying'
  }

  if (type.indexOf('numeric') != -1) {
    return 'numeric';
  }

  return type;
}




module.exports = {
  init : init,
  upgrade : upgrade,
  downgrade : downgrade,

  genDiff : genDiff
};
