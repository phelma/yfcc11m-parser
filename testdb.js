'use strict';

var db = require('./db');

db.init(function(err) {
  setInterval(function() {
    // let tag = Math.random().toString(36).substring(2,3);
    let tag = 'myTag';
    db.addTag(tag, function(err) {
      console.log('added', tag);
    })
  }, 2000)
})
