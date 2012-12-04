var http = require('http'),
    async = require('async'),
    events = require('events'),
    util = require('util'),
    request = require('request'),
    vm = require('vm'),
    EventEmitter = events.EventEmitter;

function Client(baseUrl) {
  EventEmitter.call(this);
  this.baseUrl = baseUrl;
  this.maps = {};
};
util.inherits(Client, EventEmitter);
exports.Client = Client;

exports.connect = function connect(url, callback) {
  var client = new Client(url);
  client.connect(callback);
  return client;
};

Client.prototype.connect = function connect(callback) {
  if (!callback) callback = function() {};

  // XXX: Heavy-looping, rewrite it
  var self = this;
  function cycle() {
    self.getJobs(function(err, jobs) {
      if (err) throw err;

      // No jobs, retry later
      if (jobs.length === 0) return setTimeout(cycle, 500);

      async.forEach(jobs, function(job, callback) {
        var out = job.map(job.input);
        self.putJob(job.id, out, callback);
      }, function() {
        setTimeout(cycle, 100);
      });
    });
  }
  cycle();
};

Client.prototype.request = function _request(method, url, body, callback) {
  if (typeof url === 'function') {
    callback = url;
    url = method;
    method = 'GET';
    body = null;
  } else if (typeof body === 'function') {
    callback = body;
    body = null;
  }

  var options = {
    method: method,
    url: this.baseUrl + url,
    body: body && JSON.stringify(body),
    headers: body ? {
      'Content-Type': 'application/json'
    } : null
  };
  request(options, function(err, res, body) {
    if (err) return callback(err);
    try {
      body = JSON.parse(body);
    } catch(e) {
    }

    var err = null;
    if (res.statusCode < 200 || res.statusCode >= 300) {
      err = new Error('Status code: ' + res.statusCode);
    }
    callback(err, body);
  });
};

Client.prototype.getJobs = function getJobs(loadMaps, callback) {
  if (typeof loadMaps === 'function') {
    callback = loadMaps;
    loadMaps = true;
  }

  var self = this;
  this.request('/jobs', function(err, jobs) {
    if (err || ! loadMaps) return callback(err, jobs);

    var unknown = {};
    jobs.forEach(function(job) {
      if (self.maps.hasOwnProperty(job.map)) return;
      unknown[job.map] = true;
    });

    async.forEach(Object.keys(unknown), function(mapId, callback) {
      self.getMap(mapId, function(err, map) {
        if (err) return callback(err);
        // Map will be in self.maps
        callback(null);
      });
    }, function() {
      jobs.forEach(function(job) {
        job.map = self.maps[job.map];
      });
      callback(null, jobs);
    });
  });
};

Client.prototype.getMap = function getMap(id, callback) {
  var self = this;

  this.request('/map/' + id, function(err, map) {
    if (err) return callback(err);

    if (!self.maps.hasOwnProperty(id)) {
      try {
        self.maps[id] = vm.runInNewContext('(' + map.source + ')', 'map');
      } catch (e) {
        if (e) return callback(e);
      }
    }
    callback(null, self.maps[id]);
  });
};

Client.prototype.putJob = function putJob(id, out, callback) {
  this.request('PUT', '/job/' + id, out, callback);
};
