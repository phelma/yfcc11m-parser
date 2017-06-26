'use strict';

let redis = require('redis');
let readline = require('readline');
let fs = require('fs');
let async = require('async');
let csvWriter = require('csv-write-stream');

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

var clearDb = false;
var host = '192.168.1.6';
let whiteList = new Set();
let whiteListArr = new Array();

module.exports = {
  init(idIn, done) {
    id = idIn;
    client = redis.createClient({host:host});

    async.series([
      function(next) {
        client.on('connect', () => {
          next();
        });
      },
      function(next) {
        if(clearDb) {
          client.flushdb( function(err, success) {
            console.log('Database cleared ' + success);
            next();
          });
        } else {
          next();
        }
      },
      function(next) {
        // read the white list tags
        const rl = readline.createInterface({
          input: fs.createReadStream('tag_whitelist.txt')
        });
        rl.on('line', function (line) {
          whiteList.add(line.toLowerCase());
        });
        rl.on('close', function() {
          whiteListArr = Array.from(whiteList);
          next();
        });
      },
      function(next) {
        done();
      }]);
  },

  scan(pattern, cursor, callback) {
    client.scan(cursor, 'MATCH', pattern, 'COUNT', 10000, function(err, reply) {
      if(err) {
        console.log(err);
        throw err;
      }
      cursor = reply[0];
      if(cursor === '0') {
        callback(null);
      }
      else {
        callback(reply[1], function() {
          setImmediate(function() {
            module.exports.scan(pattern, cursor, callback);
          });
        });
      }
    });
  },

  getArray(key, callback) {
    client.lrange(key, 0, -1, function(err, val) {
      callback(err, val);
    });
  },

  tokenise(strIn) {
    let out = strIn.toLowerCase();
    // remove chars that are not letters or '-'
    out = out.replace(/[^a-zA-Z\-]/g, ' ');
    return out.split(' ');
  },

  countOccurance(lookFor, lookIn) {

    let tokens = module.exports.tokenise(lookIn);

    if(typeof lookFor === 'string') { 
      let cnt = 0; 
      for(let i=0; i < tokens.length; ++i) {
        if(tokens[i] === lookFor) {
          cnt++;
        }
      }
      return cnt;
    }
    else {
      let cnts = [];
      for(let j=0; j < lookFor.length; ++j) {
        cnts.push(0);
        for(let i=0; i < tokens.length; ++i) {
          if(tokens[i] === lookFor[j]) {
            cnts[j]++;
          }
        }
      }
      return cnts;
    }
  },

  addTag(tagString, url, title, description, tagOrder, done) {

    let tagLower = tagString.toLowerCase();
    
    if(whiteList.size === 0 || whiteList.has(tagLower)) {
      
      let titleCnt = module.exports.countOccurance(tagLower, title);
      let descriptionCnt = module.exports.countOccurance(tagLower, description);

      async.waterfall([
        function(next) {
          client.incr(`tagCount_:${tagString}`, function(err, val) {
            next(err, val);
          });
        },
        function(value, next) {
          if(value % 1000 === 0) {
            console.log(tagString + ' tagged in ' + value.toString() + ' images');
          }
          client.lpush([`${tagString}_${value}`,
            url,
            titleCnt,
            descriptionCnt,
            tagOrder], next);
        }],
      function(err) {
        done();
      });
    } else {
      done();
    }
  },

  addTitleAndDescription(title, description, done) {
    if(whiteListArr.length > 0) {

      let titleCnt = module.exports.countOccurance(whiteListArr, title);
      let descriptionCnt = module.exports.countOccurance(whiteListArr, description);

      async.eachOf(whiteListArr, function(item, index, nextWhiteList) {
        async.series([
          function(next) {
            if(titleCnt[index] > 0) {
              client.incr(`titleCount_:${whiteListArr[index]}`, function(err, val) {
                next();
              });
            } else {
              next();
            }
          },
          function(next) {
            if(descriptionCnt[index] > 0) {
              client.incr(`DescrCount_:${whiteListArr[index]}`, function(err, val) {
                next();
              });
            } else {
              next();
            }
          }],
        function() {
          nextWhiteList();
        });
      },
      function(err) {
        done();
      });
    }
    else {
      done();
    }
  },

  dumpWhiteListUrlsToCsv(done) {
    let out = csvWriter();
    out.pipe(fs.createWriteStream('./urls.csv'));

    async.eachSeries(whiteList,
      function(tag, next) {
        client.lrange(tag, 0, 10000, function(err, value) {
          if(value != null) {
            for(let i=0; i < value.length; i++) {
              out.write({tag: tag, source: 'flickr-100M', url: value[i]});
            }
          }
          next();
        });
      },
      function() {
        out.end();
        done();
      });
  },

  done() {
    client.quit();
  }
};
