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

if (!exists) {
  db.serialize(function(){
      db.run('CREATE TABLE atoms (server_index integer primary key, collection text, client text, client_index integer, value text)');
      db.run('CREATE UNIQUE INDEX unique_atoms on atoms(collection, client, client_index)');
  });
}

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

function insert(records, client_id, cb) {
  let placeholders = new Array(records.length).fill('(?,?,?,?)').join(',');
  let each = records.map(d => [d.collection, client_id, d.client_index, JSON.stringify(d.value)]);
  let all = [].concat(...each);
  if (cb) {
    db.run('INSERT OR IGNORE into atoms (collection, client, client_index, value) VALUES ' + placeholders, all, cb);
  } else {
    db.run('INSERT OR IGNORE into atoms (collection, client, client_index, value) VALUES ' + placeholders, all);
  }
}

io.on('connection', function(socket){
  if (!socket.request.headers.cookie) {
    console.log('no auth cookie!!!');
  } else {
    socket.client_id = cookie.parse(socket.request.headers.cookie).live_id;
    console.log('authed', socket.client_id);
  }

  socket.on('ask', function(collections, next) {
    console.log(socket.client_id, 'askin bout', collections);
    let subscriptions = Object.keys(collections);
    subscriptions.forEach(s => socket.join(s));

    let AFTER = new Array(subscriptions.length).fill('(collection = ? AND server_index >= ?)').join(' OR ');
    let values = Object.entries(collections).reduce((a, b) => a.concat(b), []);

    db.all('SELECT collection, server_index, client, client_index, value FROM atoms WHERE client != ? AND ('+AFTER+')', socket.client_id, ...values, function(err, data) {
      if (err) { throw err; }
      console.log(`Found ${data.length} since`, collections);
      data.forEach(d => d.value = JSON.parse(d.value));
      socket.emit('tell', data);
    });
  });

  socket.on('tell', function(data, ack) {
    db.serialize(function() {
      if (data.length === 0) { return; }
      console.log('heard tell', data);
      let records = data.slice();
      while (records.length > 249) {
        insert(records.splice(0,249), socket.client_id);
      }
      insert(records, socket.client_id, function(err) {
        if (err) { throw err; }
        ack();
        let clock_base = {};
        for (let item of data) {
          if (clock_base[item.collection]) {
            clock_base[item.collection] = Math.min(item.client_index, clock_base[item.collection]);
          } else {
            clock_base[item.collection] = item.client_index;
          }
        }

        for (let collection in clock_base) {
          db.all('SELECT server_index, collection, client, client_index, value FROM atoms WHERE client = ? AND collection = ? AND client_index >= ?', socket.client_id, collection, clock_base[collection], function(err, results) {
            if (err) { throw err; }
            results.forEach(d => d.value = JSON.parse(d.value));
            socket.in(collection).emit('tell', results);
          });
        }
      });
    });
  });
});

setInterval(function() {
  console.log('connected clients', Object.values(io.sockets.connected).map(s => s.client_id));
}, 5000);

http.listen(3001, function(){
  console.log('listening on *:3001');
});
