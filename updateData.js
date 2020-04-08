const fs = require('fs');
const readline = require('readline');
const https = require('https');
const zlib = require('zlib');
const path = require('path');
const sqlite3 = require('sqlite3');
const {execSync} = require('child_process');

const DESTFILE = path.join(__dirname, 'fileFromIMDB.tsv.gz');
const UNZIPFILE = path.join(__dirname, 'fileUnzipped.tsv');
const DATABASE = path.join(__dirname, 'imdb.db');

const downloadFile = new Promise((resolve, reject) => {
   if(fs.existsSync(DESTFILE)) fs.unlinkSync(DESTFILE);
   const file = fs.createWriteStream(DESTFILE, {
      flags: "w"
   });

   const request = https.get('https://datasets.imdbws.com/title.basics.tsv.gz', response => {
      if (response.statusCode === 200) {
         console.log("Downloading file from IMDB")
         response.pipe(file);
      } else {
         file.close();
         fs.unlink(DESTFILE, () => {}); // Delete temp file
         reject(`Server responded with ${response.statusCode}: ${response.statusMessage}`);
      }
   });

   request.on("error", err => {
      file.close();
      fs.unlink(DESTFILE, () => {}); // Delete temp file
      reject(err.message);
   });

   file.on("finish", () => {
      resolve(DESTFILE);
   });

   file.on("error", err => {
      file.close();
      fs.unlink(DESTFILE, () => {}); // Delete temp file
      reject(err.message);
   });
}).then(file => {
   const unzipFile = new Promise((resolve, reject) => {
      if(fs.existsSync(UNZIPFILE)) fs.unlinkSync(UNZIPFILE);
      const zipped = fs.createReadStream(file);
      const unzipped = fs.createWriteStream(UNZIPFILE, {
         flags: "w"
      })
      const gz = zlib.createGunzip();
      console.log("File downloaded, beginning to unzip");
      zipped.pipe(gz).pipe(unzipped).on('finish', (err) =>{
         if(err) reject(err);
         else resolve(UNZIPFILE);
      })
   }).then(file => {
      console.log("File Unzipped, creating database");
      const makeDatabase = new Promise((resolve, reject) => {
         if(fs.existsSync(DATABASE))fs.unlinkSync(DATABASE);
         let db = new sqlite3.Database(DATABASE, (err) => {
            if(err) reject(err);
         });
         db.run('CREATE TABLE imdb(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)', (err) => {
            if(err) reject(err);
            const reader = readline.createInterface({
               input: fs.createReadStream(UNZIPFILE)
            });
            reader.on('line', (line) => {
               db.run(`INSERT INTO imdb VALUES("${line.replace(/"/g, "").replace(/\t/g, '","')}")`, (err) => {
                  if(err){
                     console.log(line);
                     console.log(line.replace(/"/g, "").replace(/\t/g, '","'));
                     reject(err);
                  }
               });
            });
            reader.on('close', () => {
               db.close();
               console.log("Database created");
               resolve(DATABASE);
            })
         })
      })
   })
}).catch(err => console.error(err));