'use strict';

let config = {
  url: 'mongodb://localhost:27017/yfcc-tags',
};

let mongoose = require('mongoose');
let mongodb = require('mongodb');


let tagSchema = mongoose.Schema({
  name: String,
  count: Number
});

let Tag = mongoose.model('Tag', tagSchema);

let assert = require('assert');

let db;

let quickClient;
let quickDb;
let quickCollection;

module.exports = {
  init(done){
    mongoose.connect(config.url);
    db = mongoose.connection;
    db.once('open', function() {
      done();
    });
  },

  addTag(tagString, done){
    Tag.findOne({name: tagString}, function(err, res) {
      if (err){
        done && done(err);
        return;
      }
      if (res) {
        res.update({ $inc: {count: 1}}, function(err) {
          done && done(err);
        });
      } else {
        let tag = new Tag({name: tagString, count: 1});
        tag.save(function (err) {
          done && done(err);
        });
      }
    })
  },

  addBoth(tagString, count, done){
    let tag = new Tag({name: tagString, count: count})
    tag.save(function(err) {
      done && done(err);
    });
  },

  quit(){
    db.close();
  },

  initQuick(done){
    var quickClient = mongodb.MongoClient;
    quickClient.connect(config.url, function(err, db) {
      if(err){
        console.log('error')
        console.error(err);
        console.trace();
      }
      quickCollection = db.collection('tags');
      done && done(err);
    });
  },

  addBothQuick(tagString, count, done){
    quickCollection.insert({
      name: tagString,
      count: Number(count)
    }, function(err, res) {
      done && done(err);
    });
  },

  getQuick(count, offset, done){
    console.log('getting')
    quickCollection
      .find({})
      .sort({count: -1})
      .limit(count || 10)
      .skip(offset || 0)
      .toArray(function (err, docs) {
        done && done(err, docs);
      });
  },

  quitQuick(){
    //quickDb.close();
  }

    // Tag.update({name: tagString}, {$inc: {count: 1}}, {upsert: true}, function(err, res) {
    //   done && done(err);
    // });

    // collection.insertOne({
    //   tag: tag,
    //   count: 1
    // }, function(err, r) {
    //   assert(!err);
    //   console.log('inserted', tag);
    //   done && done(err, r);
    // });
}
