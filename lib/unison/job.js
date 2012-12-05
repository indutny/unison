// Model start
exports.create = function create(scheduler) {

var async = require('async'),
    crypto = require('crypto'),
    events = require('events'),
    util = require('util'),
    unison = require('../unison'),
    EventEmitter = events.EventEmitter;

// Calculate some common prefixes here
var db = scheduler.db,
    prefix = {
      hash: db.prefix + 'j/h',
      set: db.prefix + 'j/s',
      queue: db.prefix + 'j/q',
      replica: db.prefix + 'j/r/',
      revReplica: db.prefix + 'j/rr',
      solutionsRank: db.prefix + 'j/sr/',
      solutionsHash: db.prefix + 'j/sh/',
      complete: db.prefix + 'j/c/',

      // Regexp for matching redis pub/sub events
      completeR: new RegExp('^' +
                            db.prefix.replace(/([^\w\d])/g, '\\$1') +
                            'j\\/c\\/(.*)$')
    };

//
// ### function Job (options)
// #### @options {Object} instance data
// Job constructor.
//
function Job(options) {
  EventEmitter.call(this);

  this.id = options.id;
  this.input = options.input;
  this.map = scheduler.map.create(options.map || '');
  this.continuous = options.continuous === true;

  // Number of iteration, useful only in continuous jobs
  this.iteration = options.iteration || 0;

  if (!this.id) throw new Error('Job\'s id is required');
  if (!this.input) throw new Error('Job\'s input is required');
  if (!this.map) throw new Error('Job\'s map function is required');
};
util.inherits(Job, EventEmitter);
Job.create = function create(options) {
  return new Job(options);
};

//
// ### function toJSON ()
// Convert instance to json
//
Job.prototype.toJSON = function toJSON() {
  return {
    id: this.id,
    input: this.input,
    map: {
      id: this.map.id
    },
    iteration: this.iteration,
    continuous: this.continuous
  };
};

//
// ### function qid ()
// This is somewhat hacky. We're using `id/iteration` to prevent users from
// submitting stale iteration results.
//
Job.prototype.qid = function qid() {
  return this.id + '/' + this.iteration;
};

//
// ### function parseQid (qid)
// #### @qid {String} Either id or qid
// Parse qid and return id and iteration
//
Job.parseQid = function parseQid(qid) {
  var match = qid.match(/^(.*)\/(\d+)$/);
  if (match === null) return { id: qid, iteration: null };

  return {
    id: match[1],
    iteration: parseInt(match[2], 10)
  };
};

//
// ### function load (job, callback)
// #### @job {Job|String} Either job object, qid or id
// #### @callback {Function} *optional* Continuation
// Load job from database.
//
Job.load = function load(job, callback) {
  if (!callback) callback = function() {};

  var self = this,
      qid = Job.parseQid(job.id || job),
      id = qid.id;

  db.hget(prefix.hash, id, function(err, job) {
    if (err) return callback(err);
    if (!job) return callback(new Error('not found'));

    job = Job.create(JSON.parse(job));

    // If was requested to load job by qid, check that iteration match current.
    if (qid.iteration === null || qid.iteration === job.iteration) {
      callback(null, job);
    } else {
      callback(new Error('Iteration mistmatch'));
    }
  });
};

//
// ### function loadReplica (rid, callback)
// #### @rid {String} Replica identificator
// #### @callback {Function} *optional* Continuation to proceed to
// Find matching replica and load job by qid
//
Job.loadReplica = function loadReplica(rid, callback) {
  db.hget(prefix.revReplica, rid, function(err, id) {
    if (err) return callback(err);
    if (!id) return callback(new Error('Replica not found'));

    Job.load(id, callback);
  });
};

//
// ### function save (callback)
// #### @callback {Function} Continuation to proceed too
// Save job into database.
//
// Details:
//
// * Add job to the hash.
// * Add job to the queue and remove any previous iterations of job from the
//   queue.
//
Job.prototype.save = function save(callback) {
  if (!callback) callback = function() {};

  var self = this,
      data = JSON.stringify(this.toJSON()),
      transaction = db.multi();

  // Make everything in one transaction
  transaction.hset(prefix.hash, this.id, data);
  transaction.smembers(prefix.queue);
  transaction.sadd(prefix.queue, this.qid());

  // Save map, but do not overwrite it
  if (this.map.source) this.map.save(this, transaction);
  transaction.exec(function(err, results) {
    if (err) return callback(err);

    // Remove previous members of queue
    var previous = results[1];

    previous.filter(function(id) {
      var p = Job.parseQid(id);
      return p.id === self.id && p.iteration !== self.iteration;
    }).reduce(function(transaction, id) {
      return transaction.srem(prefix.queue, id);
    }, db.multi()).exec(callback);
  });
};

//
// ### function remove (dequeue, callback)
// #### @dequeueOnly {Boolean} *optional* (default: false)
// #### @callback {Function} Continuation
// Removes job from queues and hashmap (if not dequeueOnly)
//
Job.prototype.remove = function remove(dequeueOnly, callback) {
  if (typeof dequeue === 'callback') {
    callback = dequeue;
    dequeue = false;
  }
  if (!callback) callback = function() {};

  var self = this,
      transaction = db.multi();

  // Remove from hashmap
  if (!dequeueOnly) transaction.hdel(prefix.hash, this.id, data);

  // Remove from queue and from cleaner set
  transaction.srem(prefix.queue, this.qid());
  transaction.zrem(prefix.set, this.qid());

  // Remove all solutions
  transaction.del(prefix.solutionsRank + this.qid());
  transaction.del(prefix.solutionsHash + this.qid());

  // And get all replicas
  transaction.smembers(prefix.replica + this.qid());

  // Delete all replica mappings and reverse mappings
  transaction.exec(function(err, results) {
    var members = results.pop();
    if (!members) return callback(null);

    var transaction = db.multi();

    members.forEach(function(rid) {
      transaction.hdel(prefix.revReplica, rid);
    });
    transaction.del(prefix.replica + self.qid());
    transaction.exec(function() {
      callback(null);
    });
  });
};

//
// ### function replicate (callback)
// #### @callback {Function} *optional* Continuation
// Create new replica and return it's random key
//
Job.prototype.replicate = function replicate(callback) {
  if (!callback) callback = function() {};

  var key = this.qid(),
      rand = crypto.randomBytes(16).toString('hex');

  // Insert replica into cleaner's set
  db.multi()
    .zadd(prefix.set, +new Date, rand + '/' + key)

    // Create mapping and reverse mapping
    .sadd(prefix.replica + key, rand)
    .hset(prefix.revReplica, rand, key)
    .exec(callback);

  return rand;
};

//
// ### function dereplicate (rid, callback)
// #### @rid {String} Replica id
// #### @callback {Function} *optional* Continuation
// Remove replica
//
Job.prototype.dereplicate = function dereplicate(rid, callback) {
  Job.dereplicate(this, rid, callback);
};

//
// ### function dereplicate (job, rid, callback)
// #### @job {Object} Job configuration
// #### @rid {String} Replica id
// #### @callback {Function} *optional* Continuation
// Remove replica
//
Job.dereplicate = function dereplicate(job, rid, callback) {
  if (!callback) callback = function() {};

  if (typeof job !== 'object') throw new Error('wtf');
  var key = Job.prototype.qid.call(job);

  db.multi()
    .srem(prefix.replica + key, rid)
    .hdel(prefix.revReplica, rid)
    .exec(callback);
};

//
// ### function addSolution (rid, solution, callback)
// #### @rid {String} Replica id
// #### @solution {Value} Calculated solution
// #### @callback {Function} *optional* Continuation
// Add solution to the list and check if we've found correct one.
//
Job.prototype.addSolution = function addSolution(rid, solution, callback) {
  if (!callback) callback = function() {};

  if (!solution) {
    return callback(new Error('Solution is incorrect'));
  }

  var self = this,
      out = JSON.stringify(solution);

  async.waterfall([function(callback) {
    self.dereplicate(rid, function(err) { callback(err) });
  }, function(callback) {
    var hash = crypto.createHash('sha1').update(out).digest('hex'),
        key = prefix.solutionsRank + self.qid();

    db.multi()
      .zincrby(key, 1, hash)
      .hset(prefix.solutionsHash + self.qid(), hash, out)
      .zrevrangebyscore(key,
                        'inf',
                        0,
                        'WITHSCORES')
      .exec(callback);
  }, function(results, callback) {
    var list = results[2],
        max = 0,
        maxKey,
        total = 0;

    for (var i = 0; i < list.length; i += 2) {
      var rank = parseInt(list[i + 1], 10);
      total += rank;
      if (rank > max) {
        max = rank;
        maxKey = list[i];
      }
    }

    // If consensus is reached
    if (max < scheduler.R) return callback(null, false);

    async.waterfall([function(callback) {
      // Get correct solution
      db.hget(prefix.solutionsHash + self.qid(), maxKey, callback);
    }, function(o, callback) {
      // Remove job
      self.remove(true, function() {
        // Notify listeners about result
        db.publish(prefix.complete + self.qid(), o || out);
        callback(null, true);

        // And restart job if it's continuous
        if (!self.continuous) return;
        try {
          self.input = JSON.parse(o || out);
        } catch (e) {
        }
        self.iteration++;
        scheduler.add(self);
      });
    }], callback);
  }], callback);
};

//
// ### function loadRandom (N, callback)
// #### @N {Number} Number of items to load
// #### @callback {Function} *optional* continuation
// Load random items and return their replicas
//
Job.loadRandom  = function loadRandom(N, callback) {
  if (!callback) callback = function() {};

  var self = this,
      jobs = [];

  async.whilst(function() {
    return jobs.length < N;
  }, function(callback) {
    db.srandmember(prefix.queue, function(err, item) {
      if (err) return callback(err);
      if (!item) return callback(new Error('Empty set'));

      Job.load(item, function(err, job) {
        if (err) return callback(err);

        // Retry if job was removed while getting it
        if (job) jobs.push(job);
        callback(null);
      });
    });
  }, function(err) {
    callback(null, jobs);
  });
};

// Listen for various events
var subscriber = scheduler.db.getSubscriber();
subscriber.on('pmessage', function(pattern, channel, message) {
  var match = channel.match(prefix.completeR);
  if (match === null) return;

  var id = match[1],
      out = JSON.parse(message);

  scheduler.emit('complete', Job.parseQid(id).id, out);
});
subscriber.psubscribe(prefix.complete + '*');

// Cleanup loop
function cleanQueue() {
  function callback() {
    // And loop after interval
    setTimeout(function() {
      cleanQueue();
    }, scheduler.jobTimeout);
  }

  var end = +new Date - scheduler.jobTimeout;
  db.zrangebyscore(prefix.set, 0, end, function(err, items) {
    if (err) return callback(err);

    // Filter out expired items
    var expired = items.map(function(id) {
      var match = id.match(/^([\w\d]+)\/(.*)\/(\d+)$/);
      if (!match) return { all: id };

      return {
        all: id,
        rand: match[1],
        id: match[2],
        iteration: parseInt(match[3], 10)
      };
    });

    // And return them back
    async.forEach(expired, function(item, callback) {
      db.zrem(prefix.set, item.all, function() {
        if (!item.rand) return callback(null);
        Job.dereplicate(item, item.rand, callback);
      });
    }, callback);
  });
};
cleanQueue();

return Job;

// Model end
};
