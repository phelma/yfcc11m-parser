'use strict';

let redis = require('redis');

// client.on('connect', () => {
//   console.log('connected');
//   client.incr('blah', (err, res) => {
//     client.get('blah', (err, res) => {
//       console.log(res);
//       client.quit();
//     })
//   });
// })

let client;
let id = '';

module.exports = {
  init(idIn, done){
    id = idIn;
    client = redis.createClient();
    client.on('connect', () => {
      done();
    })
  },

  addTag(tagString, done){
    client.incr(`tag:${id}:${tagString}`, done)
  },

  done(){
    client.quit();
  }
}
