'use strict';

let pg = require('pg');
let async = require('async');

let databaseUrl = 'postgres://visp:visp@127.0.0.1:5432/yf100m';

module.exports = {
  init(){

  },

  addRow(tag, count, cb){
    let query_str = 'INSERT INTO tags ($1, $2) as (tag, count)';
    let query_vals = [tag, count];
  }
}
