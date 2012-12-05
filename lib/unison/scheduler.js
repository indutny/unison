var unison = require('../unison'),
    express = require('express'),
    events = require('events'),
    redis = require('redis'),
    util = require('util'),
    Job = unison.job.Job,
    EventEmitter = events.EventEmitter;

var scheduler = exports;

//
// ### function Scheduler (options)
// #### @options {Object} options
// Scheduler constructor
//
function Scheduler(options) {
  EventEmitter.call(this);

  // Minimum number of equal results to consider them correct
  this.R = options.R;

  // Number of jobs to give to the client
  this.W = options.W;

  // Job invalidation time
  this.jobTimeout = options.jobTimeout || 5000;

  if (!this.R || !this.W) {
    throw new Error('R and W are required to create Scheduler');
  }

  // Create express router and middleware
  this.router = express();

  this.initDb(options.redis || {});
  this.init();
};
util.inherits(Scheduler, EventEmitter);
scheduler.Scheduler = Scheduler;
scheduler.create = function create(options) {
  return new Scheduler(options);
};

//
// ### function init ()
// Initialize router and other stuff
//
Scheduler.prototype.init = function init() {
  this.router.use(express.bodyParser());

  this.router.get('/unison/jobs', this.getJobs.bind(this));
  this.router.get('/unison/map/:id', this.getMap.bind(this));
  this.router.put('/unison/job/:id', this.handleSubmit.bind(this));
};

//
// ### function initDb (options)
// #### @options {Object} database options
// Initializes database
//
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

//
// ### function middleware ()
// Return middleware
//
Scheduler.prototype.middleware = function middleware() {
  return this.router;
};

//
// ### function add (job, callback)
// #### @job {Job} job
// #### @callback {Function} continuation
// Enqueues job
//
Scheduler.prototype.add = function add(job, callback) {
  job.save(callback);
};

//
// ### function remove (job, callback)
// #### @job {Job} job
// #### @callback {Function} continuation
// Dequeues job
//
Scheduler.prototype.remove = function remove(job, callback) {
  job.remove(callback);
};

//
// ### function getJobs (req, res)
// #### @req {Request}
// #### @res {Response}
// /unison/jobs handler
//
Scheduler.prototype.getJobs = function getJobs(req, res) {
  var self = this;
  this.job.loadRandom(this.W, function(err, jobs) {
    if (err) return res.json(500, { err: err.toString() });

    // If job won't be completed by the client - it should be pushed back to
    // the storage.
    jobs = jobs.map(function(job) {
      return {
        id: job.replicate(),
        map: job.map.id,
        input: job.input
      };
    });

    res.json(jobs);
  });
};

//
// ### function getMap (req, res)
// #### @req {Request}
// #### @res {Response}
// /unison/map/:id handler
//
Scheduler.prototype.getMap = function getMap(req, res) {
  res.setHeader('Expires', 'Thu, 01 Jan 2280 00:00:00 GMT');
  res.setHeader('Cache-Control', 'public');
  this.map.load(req.params.id, function(err, map) {
    res.json({ source: map });
  });
};

//
// ### function getSubmit (req, res)
// #### @req {Request}
// #### @res {Response}
// PUT /unison/job/:id handler
//
Scheduler.prototype.handleSubmit = function handleSubmit(req, res) {
  this.job.loadReplica(req.params.id, function(err, job) {
    if (err) {
      return res.json(404, { error: err.toString() });
    }

    // Process solution
    job.addSolution(req.params.id, req.body, function() {
      // Reply
      res.json({ ok: true });
    });
  });
};
