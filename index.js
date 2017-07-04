'use strict';

let fs = require('fs');
let parse = require('csv-parse');
let id = require('shortid').generate();
let bz2 = require('unbzip2-stream');
let async = require('async');
console.log('id', id);

let db = require('./redis');
let keywordDumper = require('./keyword-exporter');

String.prototype.replaceAll = function(search, replacement) {
  var target = this;
  return target.split(search).join(replacement);
};

let maxQueries = 10000;
let totalCnt = 0;

function set(n, callback) {
  let count = 0;
  let opts = {
    delimiter: '\t',
    relax_column_count: true
  };
  let paused = false;
  let dbQueries = 0;
  let parser = parse(opts);
  let index = n || 0;
  let input = fs.createReadStream(__dirname + '/YFCC100M/yfcc100m_dataset-' + index)
    .pipe(parser);

  console.time('TIME');
  input.on('end', () => {
    console.timeEnd('TIME');
    console.log('ID ' + id);
    callback();
  });

  parser.on('data', (data) => {
    if (!data) {
      return;
    }
    if(totalCnt === 0) {
      console.time('10k');
    }
    ++totalCnt;
    if (++count % 10000 === 0){
      console.log(count, data[8]);
      console.timeEnd('10k');
      console.time('10k');
    }

    let tags = data[8];
    let url = data[14];
    let title = data[6];
    let description = data[7];

    tags = tags.split(',')
      .filter(item => {
        return item;
      });

    dbQueries += tags.length;
    if (dbQueries >= maxQueries && !paused) {
      paused = true;
      input.pause();
    }

    async.series([
      function(next) {
        //db.addTitleAndDescription(title, description, next);
        next();
      },
      function(next) {

        if (!tags || tags.length === 0) {
          next();
          return;
        }

        async.eachOf(tags, (item, index, cb) => {

          if(index < 15) {
            db.addTag(item, url, title, description, index, function(err) {
              cb(err);
            });
          }
          else {
            cb();
          }
        },
        function() {
          next();
        });
      }],
    function() {
      dbQueries -= tags.length;
      if (dbQueries < maxQueries && paused) {
        paused = false;
        input.resume();
      }
    });
  });

  parser.on('warning', (err) => {
    console.error('WARNING', err.message);
  });

  parser.on('error', (err) => {
    console.error('ERROR', err);
  });
}

db.init(id, () => {
  let dump = false;
  if(dump) {
    keywordDumper.exportUrls(function() {
      console.log('CSV File written');
    });
//    db.dumpWhiteListUrlsToCsv(function() {
//      console.log('CSV file written');
//    });
  }
  else {
    let indexes = [0,1,2,3,4,5,6,7,8,9];
    async.eachSeries(indexes, function(el, done) {
      console.log('NEXT SET ' + el);
      set(el, function() {
        console.log('SET DONE ' + el);
        done();
      });
    }, function(err) {
      console.log('ALL DONE YEAH MATE');
    });
  }
});

