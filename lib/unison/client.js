var async = require('async'),
    request = require('request'),
    vm = require('vm');

//
// ### function Client (baseUrl)
// #### @baseUrl {String} Absolute prefix url for unison REST APIs
// Client constructor
//
function Client(baseUrl) {
  this.baseUrl = baseUrl;
  this.maps = {};
  this._destroyed = false;
};
exports.Client = Client;

//
// ### function connect (url)
// #### @url {String} base url, see Client constructor
// Create client and start doing jobs
//
exports.connect = function connect(url) {
  var client = new Client(url);
  client.connect();
  return client;
};

//
// ### function connect ()
// Connect to server and start doing jobs
//
Client.prototype.connect = function connect() {
  var self = this;
  function cycle() {
    if (self._destroyed) return;

    self.cycle(cycle);
  };
  cycle();
};

//
// ### function close ()
// Stop doing jobs
//
Client.prototype.close = function close() {
  this._destroyed = true;
};

//
// ### function cycle (callback
// #### @callback {Function} *optional* Continuation
// Request jobs and execute them once
//
Client.prototype.cycle = function cycle(callback) {
  if (!callback) callback = function() {};

  var self = this;
  this.getJobs(function(err, jobs) {
    if (err) return callback(err);

    async.forEach(jobs, function(job, callback) {
      var out = job.map(job.input);
      self.submitSolution(job.id, out, callback);
    }, callback);
  });
};

//
// ### function _request (method, url, body, callback)
// #### @method {String} *optional* (default: 'GET')
// #### @url {String}
// #### @body {Value} *optional*
// #### @callback {Function}
// Wrapper around request module
//
Client.prototype._request = function _request(method, url, body, callback) {
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

//
// ### function getJobs (loadMaps, callback)
// #### @loadMaps {Boolean} *optional* (default: true) load embedded maps
// #### @callback {Function} continuation
// Get jobs from server
//
Client.prototype.getJobs = function getJobs(loadMaps, callback) {
  if (typeof loadMaps === 'function') {
    callback = loadMaps;
    loadMaps = true;
  }

  var self = this;
  this._request('/jobs', function(err, jobs) {
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

//
// ### function getMap (id, callback)
// #### @id {String} Map id
// #### @callback {Function} continuation
// Get map from server
//
Client.prototype.getMap = function getMap(id, callback) {
  var self = this;

  // Use cached map
  if (this.maps.hasOwnProperty(id)) {
    return callback(this.maps[id]);
  }

  this._request('/map/' + id, function(err, map) {
    if (err) return callback(err);

    try {
      self.maps[id] = vm.runInNewContext('(' + map.source + ')',
                                         'map#' + id);
    } catch (e) {
      if (e) return callback(e);
    }
    callback(null, self.maps[id]);
  });
};

//
// ### function submitSolution (id, out, callback)
// #### @id {String} Job id
// #### @out {Value} Solution
// #### @callback {Function} Continuation
// Submit solution to server
//
Client.prototype.submitSolution = function submitSolution(id, out, callback) {
  this._request('PUT', '/solution/' + id, out, callback);
};
