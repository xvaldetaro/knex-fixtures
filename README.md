# knex-fixtures

Populate your DB automatically with useful data with the help of fixture-factory and fakerjs

## Usage

All your fixtures should be in a package, and exposed by the topmost package.
A fixture must define its name, which is also a name of a table it will populate,
a model describing its DB entries and (optionally) dependencies on other fixtures.

For instance your fixture `index.js` could look like:

    var user = {
      name: 'users',
      model: {
        id: {
          method: 'random.number',
          options: {
            min: 1,
            max: 1e6,
            _unique: true
          }
        },
        name: 'name.lastName'
      }
    };

    var property = {
      name: 'property_addrs',
      depends: [ 'users' ],
      model: {
        address: 'address.streetName',
        belongs_to: {
          reference: 'users.id'
        }
      }
    }

    module.exports = {
      user: user,
      property: property
    };
      

Now you need to initialize fixtures with the models and a working DB connection:

    knexFixtures(require('./fixtures'), knex);
   
And populate the DB:

    knexFixtures.upgrade().then(function() {
      console.log('Upgrade was successful!');
    });

Have fun! :)

