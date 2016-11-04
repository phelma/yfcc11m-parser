'use strict';

let fs = require('fs');
let parse = require('csv-parse');
let transform = require('stream-transform');

let maxQueries = 100;

let db = require('./db');
db.init(() => {
  let count = 0;
  var out = {};
  let opts = {
    delimiter: '\t',
    relax_column_count: true
  }
  let paused = false;
  let dbQueries = 0;
  let parser = parse(opts);
  let input = fs.createReadStream(__dirname + '/10k.tsv')
    .pipe(parser);

  console.time('TIME');
  input.on('end', function() {
    console.timeEnd('TIME');
  });

  parser.on('data', (data) => {
    if (!data){
      return;
    }


    if (++ count % 1000 === 0){
      console.log(count, data[8]);
    }

    data[8]
    .split(',')
    .filter(item => {
      return item;
    })
    .forEach(item => {
      if (++dbQueries >= maxQueries && !paused){
        paused = true;
        input.pause();
      }
      db.addTag(item, function(err) {
        if (--dbQueries < maxQueries && paused){
          paused = false;
          input.resume();
        }
        err && console.log('err', err);
      });
    });
  });

  parser.on('error', (err) => {
    console.error('ERROR');
    console.error(err);
  })
})

