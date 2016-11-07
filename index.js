'use strict';

let fs = require('fs');
let parse = require('./node-csv-parse');
let id = require('shortid').generate();
let bz2 = require('unbzip2-stream');
console.log('id', id);

let db = require('./redis');

String.prototype.replaceAll = function(search, replacement) {
  var target = this;
  return target.split(search).join(replacement);
};

let maxQueries = 10000;

db.init(id, () => {
  let count = 0;
  let opts = {
    delimiter: '\t',
    relax_column_count: true
  }
  let paused = false;
  let dbQueries = 0;
  let parser = parse(opts);
  let input = fs.createReadStream(__dirname + '/yfcc100m_dataset-0')
    // .pipe(bz2())
    .pipe(parser);

  console.time('TIME');
  input.on('end', () => {
    console.timeEnd('TIME');
    console.log('ID ' + id);
    db.done();
  });

  parser.on('data', (data) => {
    if (!data){
      return;
    }

    if (++ count % 10000 === 0){
      console.log(count, data[8]);
      console.timeEnd('10k');
      console.time('10k');
    }

    data = data[8];
    if (!data){
      return
    }

    data = data.split(',')
    .filter(item => {
      return item;
    });

    dbQueries += data.length
    if (dbQueries >= maxQueries && !paused){
      paused = true;
      input.pause();
    }
    data.forEach(item => {
      item = item.replaceAll('+', ' ');
      db.addTag(item, function(err) {
        if (--dbQueries < maxQueries && paused){
          paused = false;
          input.resume();
        }
        err && console.log('err', err);
      });
    });
  });

  parser.on('warning', (err) => {
    console.error('WARNING', err.message);
  })

  parser.on('error', (err) => {
    console.error('ERROR', err);
  })
})

