'use strict';

let count = 100000;
let tag = 'tag:HkN85WAxl:'
let db = require('./db');

let id = require('shortid').generate();

let fs = require('fs');
let csvWriter = require('csv-write-stream');
let writer = csvWriter({headers: ['name', 'count']});
writer.pipe(fs.createWriteStream('out-' + count + '-' + id + '.csv'));
let jsonfile = require('jsonfile');

console.log('initialising');
db.initQuick(function (err) {
  if (err){
    console.log('db init err', err);
    return;
  }
  console.log('intied');
  db.getQuick(count, 0, function (err, res) {
    if (err){
      console.log('db get err', err);
    }
    db.quitQuick();
    console.log('got ' + res.length);
    let out = res.map((item) => {
      return {
        name: decodeURIComponent(item.name.replace(tag, '')),
        count: item.count
      };
    }).filter((item) => {
      return isNaN(item.name);
    });

    jsonfile.writeFileSync(`out-${id}.json`, out);

//    out.forEach(function(el) {
//      console.log(el);
//      writer.write(el);
//    });
    writer.end();
    // console.log(out);
    console.log("DONE");
  });
})
