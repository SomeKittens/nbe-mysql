'use strict';

var bluebird = require('bluebird');

var methods = {};

// Holy CRUD!
methods.getArticle = function(id) {
  return db.getDb(function(client) {
    return client.queryAsync('SELECT id, title, content FROM articles WHERE id = $1', [id]);
  }).get('rows').get(0);
};

methods.getAllArticles = function () {
  return db.getDb(function (client) {
    return client.queryAsync('SELECT id, title, content FROM articles');
  }).get('rows');
};

methods.saveArticle = function(article) {
  return db.getDb(function (client) {
    return client.queryAsync('UPDATE articles' +
      ' SET title = $1, content = $2' +
      ' WHERE id = $3',
      [article.title, article.content, article.id]);
  });
};

methods.deleteArticle = function(id) {
  return db.getDb(function (client) {
    return client.queryAsync('DELETE FROM articles WHERE id = $1', [id]);
  });
};

methods.createArticle = function() {
  return db.getDb(function (client) {
    return client.queryAsync('INSERT INTO articles DEFAULT VALUES RETURNING id');
  }).get('rows').get(0).get('id');
};

methods.getFrontpage = function() {
  return db.getDb(function (client) {
    return client.queryAsync('SELECT id, title, content FROM articles ORDER BY published DESC LIMIT 10');
  }).get('rows');
};

// User auth stuff
methods.getUserById = function (id) {
  return db.getDb(function (client) {
    return client.queryAsync('SELECT id, username, passwordHash FROM users WHERE id = $1', [id]);
  }).get('rows').get(0);
};

// User auth stuff
methods.getUserByName = function (username) {
  return db.getDb(function (client) {
    return client.queryAsync('SELECT id, username, passwordHash FROM users WHERE username = $1', [username]);
  }).get('rows').get(0);
};

methods.createLocalUser = function(username, password) {
  return db.getDb(function (client) {
    return client.queryAsync('INSERT INTO users (username, passwordHash) VALUES ($1, $2) RETURNING id', [username, password]);
  }).get('rows').get(0).get('id');
};

methods.init = function () {
  return db.init();
};

methods.destroy = function () {
  return db.destroy();
};

var db;

// TODO: not have a singleton db
module.exports = function(connString) {
  db = require('./db')(connString);
  // Initalize db
  return methods;
};