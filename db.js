var mysql = require('mysql')
  , bluebird = require('bluebird');

var connection, connString;

var handleDisconnect = function() {
  connection = mysql.createConnection(connString);
  // Ugly hack.  Remove.
  bluebird.promisifyAll(connection);

  connection.connect(function(err) {
    if(err) {
      console.log('error when connecting to db:', err);
      setTimeout(handleDisconnect, 2000);
    }
  });

  connection.on('error', function(err) {
    console.log('db error', err);
    if(err.code === 'PROTOCOL_CONNECTION_LOST') {
      handleDisconnect();
    } else {
      throw err;
    }
  });
};

var methods = {}
  , connString;

methods.initAll = function () {
  var client, closeDb;
  return connection.queryAsync('CREATE DATABASE IF NOT EXISTS nbe')
  .then(function(result) {
    return methods.initTables();
  });
};

methods.initTables = function() {
  return bluebird.all([
    connection.queryAsync('CREATE TABLE IF NOT EXISTS articles (' +
      ' id INT AUTO_INCREMENT NOT NULL,' +
      ' title TEXT,' +
      ' content TEXT,' +
      ' published TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' +
      ' PRIMARY KEY(id)' +
    ')'),
    connection.queryAsync('CREATE TABLE IF NOT EXISTS users (' +
      ' id SERIAL PRIMARY KEY,' +
      ' username VARCHAR(150) NOT NULL,' +
      ' passwordHash VARCHAR(60) NOT NULL' +
    ')')
  ]);
};

methods.init = function() {
  return methods.initAll();
};

methods.destroy = function () {
  var client, closeDb;
  return pg.connectAsync(connString).spread(function(dbClient, close) {
    client = dbClient;
    closeDb = close;
    return connection.queryAsync('DROP DATABASE nbe');
  })
  .finally(closeDb);
};

methods.getDb = function (fn) {
  return fn(connection);
};

module.exports = function (connectionString) {
  if (!connectionString) {
    throw new Error('Connection string is required');
  }
  connString = connectionString;

  handleDisconnect();
  return methods;
};