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

  return knex.select().table('pg_catalog.pg_tables').then(function(tables) {
    var diff = {
      up   : [],
      down : []
    };

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
        diff.up.push({ query : 'create', table : modelTable });
        diff.down.push({ query : 'drop', table : modelTable });
      }
    });

    diff.up = utils.specCreates(regFixtures, diff.up);

    _.forEach(dbTables, function(dbTable) {
      if (dbTable[0] === '_' || dbTable === 'knex_migrations') {
        return;
      }

      if (!_.contains(orderedTables, dbTable)) {
        diff.up.push({ query : 'drop', table : dbTable });
        diff.down.push({ query : 'create', table : dbTable });
      }
    });

    return knex.select().table('information_schema.columns').then(function(tableDef) {
      var dbModel = _.reduce(tableDef, function (collection, def) {
          if (def.table_name[0] === '_' || def.table_name === 'knex_migrations') {
            return collection;
          }

          collection[def.table_name] = collection[def.table_name] || { model : {}};

          var dbType = eigen(def.data_type);
          var userType = eigen(def.udt_name);

          var type = dbType !== 'USER-DEFINED' ? dbType : userType;

          // TODO: Add typeModifiers
          collection[def.table_name].model[def.column_name] = {
            type : eigen(type)
          };
          return collection;
        }, {});

      diff.down = utils.specCreates(dbModel, diff.down);
      diff.up = findAlterations(dbModel, regFixtures, diff.up, orderedTables);
      diff.down = findAlterations(dbModel, regFixtures, diff.down, orderedTables, 'down');

      return Promise.resolve(diff);
    });
  });

}

var MIGRATION_PARTS = {
  begin : "'use strict';\n\n",
  beginUp : "exports.up = function(knex, Promise) {\n\treturn new Promise(function(res) {\n" + getTabs(),
  endOps : ".then(res);\n",
  end : "\t});\n};\n\n",
  placeholder : getTabs() + '// Here goes your migration\n'
};
MIGRATION_PARTS.beginDown = MIGRATION_PARTS.beginUp.replace(/\.up/, '.down');


function getTabs(indentation) {
  var howMany = (indentation || 0) + 2;
  return new Array(howMany + 1).join('\t');
}

function genMigrations(options, knex) {
  genDiff(options, knex).then(function(diff) {
    var migration = MIGRATION_PARTS.begin;

    var drops = _.filter(diff.up, function(query) {
      return query.query === 'drop';
    });

    _.forEach(drops, function(query) {
      migration += wrapOp(migration, "knex.schema.dropTable('" + query.table + "')");
    });

    if (drops.length > 0) {
      migration += MIGRATION_PARTS.endOps;
    } else {
      migration += MIGRATION_PARTS.placeholder;
    }

    migration += MIGRATION_PARTS.end;

    migration += MIGRATION_PARTS.beginDown;

    migration += MIGRATION_PARTS.placeholder;
    // TODO down migrations
    migration += MIGRATION_PARTS.end;


    migration = beautify(migration);
    console.log(migration, JSON.stringify(diff, undefined, 2));
  });
}

function beautify(migration) {
  return migration.replace(/\t/g, '  ');
}

function wrapOp(migration, operation) {
  if (migration.length === MIGRATION_PARTS.begin.length) {
    return 'return ' + operation + '\n' + getTabs(1);
  } else {
    return '.then(function() {\n' + getTabs(2) +
      'return ' + operation + '\n' + getTabs(1) + '})';
  }
};

function findAlterations(dbModel, regFixtures, diff, modelTables, direction) {
  direction = direction || 'up';

  var dropOrCreateNames = _.pluck(diff, 'table');

  var alterableTables = _.reject(modelTables, function(table) {
    return _.contains(dropOrCreateNames, table);
  });

  _.forEach(alterableTables, function(modelTable) {

    var tableModel = regFixtures[modelTable].model;
    var tableDiff = _.reduce(tableModel, function(diff, column, columnName) {
      if (columnName[0] === '_') {
        // We don't want to look for changes in stuff that is hidden,
        // and probably not even a column
        return;
      }

      var dbColumn = dbModel[modelTable].model[columnName];
      var more = function (modelType) {
        return {
          operation : 'create',
          column : columnName,
          type : modelType,
          typeModifiers : utils.getModifiers(column)
        };
      };

      var less = function() {
        return {
          operation : 'drop',
          column : columnName
        };
      };

      if (direction === 'down') {
        var temp = less;
        less = more;
        more = temp;
      }

      if (!dbColumn) {
        // If the column is not in the database
        var modelType = eigen(utils.matchType(regFixtures, column));
        diff.push(more(modelType));
      } else if (!column) {
        // If the column is not in the model, but in the db
        diff.push(less(dbType));
      } else {
        var modelType = eigen(utils.matchType(regFixtures, column));
        var dbType = eigen(utils.matchType(dbModel, dbColumn));

        // If the colun is in the db with a different type
        if (modelType !== dbType) {
          diff.push(less(dbType));
          diff.push(more(modelType));
        }
      }

      return diff;
    }, []);

    // Make sure all DROPs are before CREATEs
    tableDiff = _.sortBy(tableDiff, function(diff) {
      return diff.operation !== 'drop';
    });

    if (tableDiff.length > 0) {
      diff.push({
        query : 'alter',
        table : modelTable,
        diff : tableDiff
      });
    }
  });

  return diff;
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

  genDiff : genDiff,
  genMigrations : genMigrations
};
