'use strict';

let config = {
  url: 'mongodb://localhost:27017/yfcc-tags',
};

let mongoose = require('mongoose');


let tagSchema = mongoose.Schema({
  name: String,
  count: Number
});

let Tag = mongoose.model('Tag', tagSchema);

let assert = require('assert');

let db;
let collection;

module.exports = {
  init(done){
    mongoose.connect(config.url);
    let db = mongoose.connection;
    db.once('open', function() {
      done();
    })
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
        })
      } else {
        let tag = new Tag({name: tagString, count: 1});
        tag.save(function (err) {
          done && done(err);
        })
      }
    });

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
}
