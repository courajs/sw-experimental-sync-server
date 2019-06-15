var fs = require('fs');

var sqlite3 = require('sqlite3').verbose();

var app = require('express')();
var http = require('http').createServer(app);
var io = require('socket.io')(http);

var dbFile = './.data/sqlite.db';
var exists = fs.existsSync(dbFile);
var db = new sqlite3.Database(dbFile);

db.serialize(function(){
  if (!exists) {
    db.run('CREATE TABLE atoms (server_index integer primary key, client text, client_index integer, value text)');
  }
});

app.get('/', function(req, res){
  res.send('hello');
});

function insertValuesForData(these, client_id) {
  let values = [];
  for (let {client_index, value} of these) {
    values.push(client_id, client_index, JSON.stringify(value));
  }
  return values;
}

io.on('connection', function(socket){
  socket.state = 'updating';
  socket.buffer = [];

  socket.on('auth', function(id) {
    console.log('authing', id);
    socket.client_id = id;
  });

  socket.on('ask', function(since) {
    db.all('SELECT server_index, client, client_index, value from atoms where client != ? and server_index > ?', socket.client_id, since, function(err, data) {
      if (err) { throw err; }
      console.log('data', data[0]);
      socket.emit('tell', data.concat(socket.buffer));
      socket.state = 'live';
      socket.buffer = [];
    });
  });

  socket.on('tell', function(data) {
    db.serialize(function() {
      let records = data.slice();
      while (records.length > 333) {
        let these = records.splice(0,333);
        db.run('INSERT into atoms (client, client_index, values) VALUES ' + new Array(333).fill('(?,?,?)').join(','), insertValuesForData(these, socket.client_id));
      }
      db.run('INSERT into atoms (client, client_index, value) VALUES ' + new Array(records.length).fill('(?,?,?)').join(','), insertValuesForData(records, socket.client_id), function(err) {
        if (err) { throw err; }
        let first = this.lastId - data.length + 1;
        data.forEach(function(rec, i) {
          rec.client = socket.client_id;
          rec.server_index = first + i;
        });
        for (let s of Object.values(io.sockets.connected)) {
          if (s === socket) {
            return;
          }
          if (s.state === 'updating') {
            s.buffer = s.buffer.push(...data);
          } else if (s.state === 'live') {
            s.emit('tell', data);
          }
        }
      });
    });
  });
});

http.listen(3001, function(){
  console.log('listening on *:3001');
});
