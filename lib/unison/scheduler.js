var unison = require('../unison'),
    express = require('express'),
    events = require('events'),
    redis = require('redis'),
    util = require('util'),
    Job = unison.job.Job,
    EventEmitter = events.EventEmitter;

var scheduler = exports;

function Scheduler(options) {
  EventEmitter.call(this);

  this.R = options.R;
  this.W = options.W;
  this.jobTimeout = options.jobTimeout || 5000;

  if (!this.R || !this.W) {
    throw new Error('R and W are required to create Scheduler');
  }

  this.router = express();

  this.initDb(options.redis || {});
  this.init();
};
util.inherits(Scheduler, EventEmitter);
scheduler.Scheduler = Scheduler;
scheduler.create = function create(options) {
  return new Scheduler(options);
};

Scheduler.prototype.init = function init() {
  this.router.get('/unison/jobs', this.getJobs.bind(this));
  this.router.get('/unison/map/:id', this.getMap.bind(this));
  this.router.get('/unison/job/:id', this.handleJob.bind(this));
};

Scheduler.prototype.initDb = function initDb(options) {
  function createClient() {
    var db = redis.createClient(options.port || 6379,
                                options.host || 'localhost');

    if (options.password) {
      this.db.auth(options.password, function(err) {
        throw err;
      });
    }

    return db;
  };
  this.db = createClient();
  this.db.prefix = options.prefix || 'un/';
  this.db.jobTimeout = this.jobTimeout;
  this.db.getSubscriber = createClient;

  this.map = unison.map.create(this);
  this.job = unison.job.create(this);
};

Scheduler.prototype.middleware = function middleware() {
  return this.router;
};

Scheduler.prototype.add = function add(job, callback) {
  job.save(callback);
};

Scheduler.prototype.remove = function remove(job, callback) {
  job.remove(callback);
};

Scheduler.prototype.getJobs = function getJobs(req, res) {
  var self = this;
  this.job.loadRandom(this.W, function(err, jobs) {
    if (err) return res.json(500, { err: err.toString() });

    // If job won't be completed by the client - it should be pushed back to
    // the storage.
    jobs = jobs.map(function(job) {
      return {
        id: job.enqueue(),
        map: job.map.id,
        input: job.input
      };
    });

    res.json(jobs);
  });
};

Scheduler.prototype.getMap = function getMap(req, res) {
  res.setHeader('Expires', 'Thu, 01 Jan 2280 00:00:00 GMT');
  res.setHeader('Cache-Control', 'public');
  this.map.load(req.params.id, function(err, map) {
    res.json({ source: map });
  });
};

Scheduler.prototype.handleJob = function handleJob(req, res) {
  this.job.loadReplica(req.params.id, function(err, job) {
    if (err) {
      return res.json(404, { error: err.toString() });
    }

    // Reply first
    res.json({ ok: true });

    // Process solution later
    job.addSolution(req.params.id, req.body || {});
  });
};
