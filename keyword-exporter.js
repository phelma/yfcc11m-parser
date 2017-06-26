'use strict';

let readline = require('readline');
let fs = require('fs');
let async = require('async');
let csvWriter = require('csv-write-stream');
let db = require('./redis');
let multimap = require('multimap');

let outputFolder = './urls/';

var writeCsv = function(name, imgs) {               
  let writer = csvWriter();
  writer.pipe(fs.createWriteStream(outputFolder + name + '_urls.csv'));

  imgs.forEach(function(img, score) {
    writer.write({tag:img.tag, url:img.url, source:'flick100m'});
  });

  writer.end();
};

module.exports = {

  exportUrls(done) {

    try {
      fs.mkdirSync(outputFolder);
    } catch(e) {
    }

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

        async.eachLimit(keywords, 1, function(word, nextword) {
          totalWords += 1;
          console.log('Exporting word ' + word + ' total so far ' + totalWords.toString());
          let topimgs = new multimap();
          console.time(word);

          let cnt = 1;
          async.whilst(
            function() {return cnt >= 0;},
            function(nextIter) {
              let key = word + '_' + cnt;
              db.getArray(key, function(err, value) {
                if(err || value == null || value.length === 0) {
                  cnt = -1;
                  writeCsv(word, topimgs);
                  nextIter();
                } 
                else {
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
                  cnt += 1
                  nextIter();
                }
              });
            },
            function() {
              nextword();
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
