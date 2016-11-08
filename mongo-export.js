'use strict';

let count = 100000;
let tag = 'tag:HkN85WAxl:'
let db = require('./db');

let id = require('shortid').generate();

let fs = require('fs');
let csvWriter = require('csv-write-stream');
let writer = csvWriter({headers: ['name', 'count']});
writer.pipe(fs.createWriteStream('out-' + count + '-' + id + '.csv'));


console.log('initialising');
db.initQuick(function () {
  console.log('intied');
  db.getQuick(count, 0, function (err, res) {
    db.quitQuick();
    let out = res.map((item) => {
      return {
        name: item.name.replace(tag, ''),
        count: item.count
      };
    })
    out.forEach(function(el) {
      console.log(el);
      writer.write(el);
    });
    writer.end();
    // console.log(out);
  });
})
