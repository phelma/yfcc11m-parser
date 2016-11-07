'use strict';

let bz2 = require('unbzip2-stream');
let fs = require('fs');

fs.createReadStream(__dirname + '/yfcc100m_dataset-0.bz2')
  .pipe(bz2())
  .pipe(process.stdout);
