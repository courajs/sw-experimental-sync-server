var fs = require('fs');

var sqlite3 = require('sqlite3').verbose();

var express = require('express');
var app = express();
var http = require('http').createServer(app);
var io = require('socket.io')(http);
var cookie = require('cookie');

var dbFile = './data/sqlite.db';
var exists = fs.existsSync(dbFile);
var db = new sqlite3.Database(dbFile);

db.serialize(function(){
  if (!exists) {
      db.run('CREATE TABLE atoms (server_index integer primary key, client text, client_index integer, value text)');
      db.run('CREATE UNIQUE INDEX unique_atoms on atoms(client, client_index)');
  }
});

app.use(require('cookie-parser')());
app.use(require('body-parser').text());
app.post('/auth', function(req, res){
  if (!req.body) {
    console.log('no body in auth request!');
    res.status(400).send('send your id!!');
    return;
  }
  res.cookie('live_id', req.body, { maxAge: 90000000, httpOnly: false });
  res.send('ok');
});

function insertValuesForData(these, client_id) {
  let values = [];
  for (let {client_index, value} of these) {
    values.push(client_id, client_index, JSON.stringify(value));
  }
  return values;
}

io.on('connection', function(socket){
  if (!socket.request.headers.cookie) {
    console.log('no cookie!!!');
  } else {
    socket.client_id = cookie.parse(socket.request.headers.cookie).live_id;
  }

  socket.on('ask', function(next) {
    db.all('SELECT server_index, client, client_index, value from atoms where client != ? and server_index >= ?', socket.client_id, next, function(err, data) {
      if (err) { throw err; }
      console.log(`asked about messages since/including ${next}, found ${data.length}`);
      data.forEach(d => d.value = JSON.parse(d.value));
      socket.emit('tell', data);
    });
  });

  socket.on('tell', function(data, ack) {
    db.serialize(function() {
      if (data.length === 0) { return; }
      console.log('heard tell', data);
      let records = data.slice();
      while (records.length > 333) {
        let these = records.splice(0,333);
        db.run('INSERT OR IGNORE into atoms (client, client_index, value) VALUES ' + new Array(333).fill('(?,?,?)').join(','), insertValuesForData(these, socket.client_id));
      }
      db.run('INSERT OR IGNORE into atoms (client, client_index, value) VALUES ' + new Array(records.length).fill('(?,?,?)').join(','), insertValuesForData(records, socket.client_id));
      let min_id = data.reduce((a,d)=>Math.min(a, d.client_index), data[0].client_index);
      console.log('min id from data', min_id);
      db.all('SELECT server_index, client, client_index, value FROM atoms WHERE client = ? AND client_index >= ?', socket.client_id, min_id, function(err, results) {
        if (err) { throw err; }
        ack();
        results.forEach(d => d.value = JSON.parse(d.value));
        console.log('persisted, ready for broadcast', results);
        for (let s of Object.values(io.sockets.connected)) {
          console.log('considered',s.client_id,'for broadcast');
          if (s === socket) {
            continue;
          }
          console.log('telling', s.client_id, results);
          s.emit('tell', results);
        }
      });
    });
  });
});

setInterval(function() {
  console.log('connected clients', Object.values(io.sockets.connected).map(s => s.client_id));
}, 10000);

http.listen(3001, function(){
  console.log('listening on *:3001');
});
