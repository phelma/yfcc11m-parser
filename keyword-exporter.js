'use strict';

let readline = require('readline');
let fs = require('fs');
let async = require('async');
let csvWriter = require('csv-write-stream');
let db = require('./redis');
let multimap = require('multimap');

let outputFolder = './urls/';

var writeCsv = function(name, imgs, done) {               
  let writer = csvWriter();
  writer.pipe(fs.createWriteStream(outputFolder + name + '_urls.csv'));

  async.eachSeries(imgs,
    function(img, nextWord) {
      writer.write({tag:img.tag, url:img.url, source:'flick100m'});
      nextWord();
    },
    function() {
      writer.end();
      done();
    });
};

module.exports = {

  exportUrls(done) {

    fs.mkdirSync(outputFolder);

    async.waterfall([
      function(next) {
        let words = new Set();

        // read the white list tags
        const rl = readline.createInterface({
          input: fs.createReadStream('keyword_export_list.txt')
        });
        rl.on('line', function (line) {
          words.add(line.toLowerCase());
        });
        rl.on('close', function() {
          next(null, words);
        }); 
      },
      function(keywords, next) {
        let totalWords = 0;

        async.eachLimit(keywords, 5, function(word, nextword) {
          totalWords += 1;
          console.log('Exporting word ' + word + ' total so far ' + totalWords.toString());
          let topimgs = new multimap();
          console.time(word);

          db.scan(word + '*', '0', function(keys, handled) {
            if(keys == null) {
              // write list out to csv
              console.timeEnd(word);
              writeCsv(word, topimgs, nextword);
            } else {

              // get the key
              async.each(keys, function(key, cb) {
                db.getArray(key,
                  function(err, value) {
                    let titleCnt = Number(value[0]);
                    let descrpCnt = Number(value[1]);
                    let tagOrder = Number(value[2]);
                    let url = value[3];
                    let score = (15 - tagOrder) + titleCnt + descrpCnt;
                    topimgs.set(score, {tag:word, url:url});

                    if(topimgs.size > 4500) {
                      // remove an image with the smallest score
                      let key = topimgs.keys().next().value;
                      let valueToDelete = topimgs.get(key)[0];
                      if(!topimgs.delete(key, valueToDelete)) {
                        console.log('failed to delte');
                      }
                      else {
                        if(topimgs.get(key).length === 0) {
                          topimgs.delete(key); 
                        }
                      }
                    }
                    cb();
                  }
                );
              },
              function(err) {
                handled();
              });
            }
          });
        },
        function() {
          next();
        });
      }],
    function(err) {
      done(err);
    });
  }
};
