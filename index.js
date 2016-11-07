'use strict';

let fs = require('fs');
let parse = require('./node-csv-parse');
let id = require('shortid').generate();
let bz2 = require('unbzip2-stream');
let async = require('async');
console.log('id', id);

let db = require('./redis');

String.prototype.replaceAll = function(search, replacement) {
  var target = this;
  return target.split(search).join(replacement);
};

let maxQueries = 10000;

function set(n, callback) {
  let count = 0;
  let opts = {
    delimiter: '\t',
    relax_column_count: true
  }
  let paused = false;
  let dbQueries = 0;
  let parser = parse(opts);
  let index = n || 0;
  let input = fs.createReadStream(__dirname + '/YFCC100M/yfcc100m_dataset-' + index)
    // .pipe(bz2())
    .pipe(parser);

  console.time('TIME');
  input.on('end', () => {
    console.timeEnd('TIME');
    console.log('ID ' + id);
    callback();
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
}

db.init(id, () => {
  let indexes = [0,1,2,3,4,5,6,7,8,9];
  async.eachSeries(indexes, function(el, done) {
    console.log('NEXT SET ' + el);
    set(el, function() {
      console.log('SET DONE ' + el);
      done();
    });
  }, function(err) {
    console.log('ALL DONE YEAH MATE')
  })
})

