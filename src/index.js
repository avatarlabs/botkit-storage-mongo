var monk = require('monk');
var _ = require('lodash');
var debug = require('debug')('botkit:db');

/**
 * botkit-storage-mongo - MongoDB driver for Botkit
 *
 * @param  {Object} config Must contain a mongoUri property and May contain a mongoOptions
 *  object containing mongo options (auth,db,server,...).
 * @return {Object} A storage object conforming to the Botkit storage interface
 */
module.exports = function(config) {
  /**
   * Example mongoUri is:
   * 'mongodb://test:test@ds037145.mongolab.com:37145/slack-bot-test'
   * or
   * 'localhost/mydb,192.168.1.1'
   */
  if (!config || !config.mongoUri) {
    throw new Error('Need to provide mongo address.');
  }

  var db = monk(config.mongoUri, config.mongoOptions);

  db.catch(function(err) {
    throw new Error(err);
  });

  var storage = {};

  var tables = ['teams', 'channels', 'users', 'history'];
  // if config.tables, add to the default tables
  config.tables &&
    config.tables.forEach(function(table) {
      if (typeof table === 'string') tables.push(table);
    });

  tables.forEach(function(zone) {
    storage[zone] = getStorage(db, zone);
  });

  // okay, need to tack on history functions
  // we need storag.history.addToHistory(message, user)
  // and
  // we need storag.history.getHistoryForUser(user, limit)

  if (storage.history) {
    debug('Adding history storage methods');

    // changing to use common signatures for botkit:
    // save: function(data, cb)
    // and 
    // find: function(data, cb)
    // and calling against storage history table
    // rather than mongoose schemas

    // also need to save Date.now
    storage.history.addToHistory = function(message, user) {
      return new Promise(function(resolve, reject) {
        //var hist = new history({userId: user, message: message});
        var hist = {userId: user, message: message, date: Date.now() };
        //hist.save(function(err) {
        storage.history.insert(hist, function (err) {
          if (err) { return reject(err) }
          resolve(hist);
        });
      });
    };

    storage.history.getHistoryForUser = function(user, limit) {
      return new Promise(function(resolve, reject) {
        //storage.history.find({userId: user}).sort({date: -1}).limit(limit).exec(function(err, history) {
          //    storage.history.find(
                var table = db.get('history');
                table.find({ userId: user }, { limit: limit, sort: { date: -1 } }, function(
                    err,
                    history
                ) {
                    console.log('Got history of ' + history.length);
                    if (err) {
                        return reject(err);
                    }
                    resolve(history.reverse());
                });
      });
    };


  } else {
    throw new Error('Unable to add history support!!');
  }

  return storage;
};

/**
 * toDot
 * transforms object to dot notation
 *
 * @param {Object} obj - object to transform
 * @param {String} div - sperator for iteration, defaults to '.'
 * @param {String} pre - prefix to attach to all dot notation strings
 * @returns {Object} - dot notation
 */
function toDot(obj, div = '.', pre) {
  if (typeof obj !== 'object') {
    throw new Error('toDot requires a valid object');
  }

  if (pre != null) {
    pre = pre + div;
  } else {
    pre = '';
  }

  const iteration = {};

  Object.keys(obj).forEach(key => {
    if (_.isPlainObject(obj[key])) {
      Object.assign(iteration, toDot(obj[key], div, pre + key));
    } else {
      iteration[pre + key] = obj[key];
    }
  });

  return iteration;
}

/**
 * Creates a storage object for a given "zone", i.e, teams, channels, or users
 *
 * @param {Object} db A reference to the MongoDB instance
 * @param {String} zone The table to query in the database
 * @returns {{get: get, save: save, all: all, find: find}}
 */
function getStorage(db, zone) {
  var table = db.get(zone);

  return {
    get: function(id, cb) {
      return table.findOne({ id: id }, cb);
    },
    // shoot, let's just use the same set notation so we're never destroying fields
    save: function(data, cb) {
      return table.findOneAndUpdate(
        {
          id: data.id
        },
        //data,
        { $set: toDot(data) },
        {
          upsert: true,
          returnNewDocument: true
        },
        cb
      );
    },
    insert: function(data, cb) {
      return table.insert(
          //{ $set: toDot(data) },
          data,
          {
              returnNewDocument: true
          },
          cb
      );
    },
    // update is basically the same as save, but allows for dot.notation to set nested objects without destroying existing values.
    update: function(data, cb) {
      return table.findOneAndUpdate(
        {
          id: data.id
        },
        { $set: data },
        {
          upsert: true,
          returnNewDocument: true
        },
        cb
      );
    },
    all: function(cb) {
      return table.find({}, cb);
    },
    find: function(data, cb) {
      return table.find(data, cb);
    },
    delete: function(id, cb) {
      return table.findOneAndDelete({ id: id }, cb);
    }
  };
}
