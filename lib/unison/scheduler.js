var unison = require('../unison'),
    express = require('express'),
    redis = require('redis'),
    Job = unison.job.Job;

var scheduler = exports;

function Scheduler(options) {
  this.N = options.N;
  this.R = options.R || (options.N >> 1);
  this.W = options.W || (options.W >> 1);
  this.jobTimeout = options.jobTimeout || 5000;

  if (!this.N) throw new Error('N is required to create Scheduler');

  this.router = express();

  this.initDb(options.redis || {});
  this.init();
};
scheduler.Scheduler = Scheduler;
scheduler.create = function create(options) {
  return new Scheduler(options);
};

Scheduler.prototype.init = function init() {
  this.router.get('/unison/jobs', this.getJobs.bind(this));
  this.router.get('/unison/map/:id', this.getMap.bind(this));
  this.router.put('/unison/job/:id', this.handleJob.bind(this));
};

Scheduler.prototype.initDb = function initDb(options) {
  this.db = redis.createClient(options.port || 6379,
                               options.host || 'localhost');
  this.db.prefix = options.prefix || 'un/';
  this.db.jobTimeout = this.jobTimeout;

  if (options.password) {
    this.db.auth(options.password, function(err) {
      throw err;
    });
  }

  this.map = unison.map.create(this);
  this.job = unison.job.create(this);
};

Scheduler.prototype.middleware = function middleware() {
  return this.router;
};

Scheduler.prototype.add = function add(job, callback) {
  job.init(this);
  job.save(callback);
};

Scheduler.prototype.remove = function remove(job, callback) {
  job.remove(callback);
};

Scheduler.prototype.getJobs = function getJobs(req, res) {
  var self = this;
  this.job.getRandom(this.W, function(err, jobs) {
    if (err) return res.json(500, { err: err.toString() });

    // If job won't be completed by the client - it should be pushed back to
    // the storage.
    jobs = jobs.map(function(job) {
      job.enqueue();

      return {
        id: job.mapping,
        map: job.map,
        input: job.input,
        iteration: job.iteration
      };
    });

    res.json(jobs);
  });
};

Scheduler.prototype.getMap = function getMap(req, res) {
  res.setHeader('Expires', 'Thu, 01 Jan 2280 00:00:00 GMT');
  res.setHeader('Cache-Control', 'public');
  this.map.get(req.params.id, function(err, map) {
    res.json({ source: map });
  });
};

Scheduler.prototype.handleJob = function handleJob(req, res) {
  this.job.get(req.params.id, function(err, job) {
    if (err) {
      return res.end(404, { error: 'Job doesn\'t exist' });
    }

    res.end({ ok: true });
  });
};
