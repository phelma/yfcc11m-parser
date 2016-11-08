'use strict';

let redis = require('redis');
let async = require('async');
let mongo = require('./db');

let count = 10000;
let insertLimit = 1000;
let total = 0;
let redisClient;
let id = '';

let match = 'tag:B1Q-ik0eg:';

mongo.initQuick(function(err) {
  if (err){
    console.log(err);
    return;
  }
  redisClient = redis.createClient();
  redisClient.on('connect', () => {
    console.log('redis connected');
    iterate();
  });
})

console.time('ALL');

let iterate = function(cursor) {
  if (cursor == 0){
    console.log("DONEALL");
    console.timeEnd('ALL');
    redisClient.quit();
    mongo.quitQuick();
    return;
  }
  cursor = cursor || 0;
  redisClient.scan(cursor, 'MATCH', match + '*', 'COUNT', count, function(err, res) {
    err && console.error(err);
    let cursor = res[0];
    let keys = res[1];
    console.time('TIME');

    async.eachLimit(keys, insertLimit, function(key, done) {
      redisClient.get(key, function(err, res) {
        err && console.error(err);
        mongo.addBothQuick(key, res, done)
      });
    }, function(err) {
      console.log('total', total += keys.length);
      console.timeEnd('TIME');
      err && console.error(err);
      console.log('done ' + count);
      setImmediate(function() {
        console.log('iterating', cursor);
        iterate(cursor);
      })
    })
  });
}
