'use strict';

var topsort        = require('topsort');
var _              = require('lodash');
var fixtureFactory = require('fixture-factory');
var Promise        = require('bluebird');


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


module.exports = {
  init : init,
  upgrade : upgrade,
  downgrade : downgrade
};
