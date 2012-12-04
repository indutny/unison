var async = require('async'),
    crypto = require('crypto'),
    events = require('events'),
    util = require('util'),
    unison = require('../unison'),
    EventEmitter = events.EventEmitter;

exports.create = function create(scheduler) {
  var db = scheduler.db,
      prefix = {
        hash: db.prefix + 'j:h',
        set: db.prefix + 'j:s',
        queue: db.prefix + 'j:q',
        replica: db.prefix + 'j:r',
        revReplica: db.prefix + 'j:rr'
      };

  function Job(options) {
    EventEmitter.call(this);

    this.id = options.id;
    this.input = options.input;
    this.map = scheduler.map.create(options.map || '');
    this.continuous = options.continuous === true;

    // Index of job replica
    this.index = options.index;
    this.replica = options.replica;
    this.time = options.time;

    // Scheduler configuration
    this.scheduler = null;

    // Number of iteration, useful only in continuous jobs
    this.iteration = 0;

    if (!this.id) throw new Error('Job id is required');
    if (!this.input) throw new Error('Job input is required');
    if (!this.map) throw new Error('Job map function is required');
  };
  util.inherits(Job, EventEmitter);
  Job.create = function create(options) {
    return new Job(options);
  };

  Job.prototype.init = function init(scheduler) {
    this.scheduler = scheduler;
  };

  Job.prototype.toJSON = function toJSON() {
    return {
      id: this.id,
      input: this.input,
      map: {
        id: this.map.id
      },
      iteration: this.iteration
    };
  };

  Job.load = function load(job, callback) {
    if (!callback) callback = function() {};

    var self = this;
    db.hget(prefix.hash, job.id || job, function(err, job) {
      if (err || !job) return callback(new Error('not found'), null);

      job = Job.create(JSON.parse(job));
      job.init(self.scheduler);
      callback(null, job);
    });
  };

  Job.loadReplica = function loadReplica(id, callback) {
    db.hget(prefix.revReplica, id, function(err, replica) {
      if (err) return callback(err);
      if (!replica) return callback(new Error('Replica not found'));

      Job.load(replica.replace(/^\d+:/, ''), callback);
    });
  };

  Job.prototype.save = function save(callback) {
    var data = JSON.stringify(this.toJSON());

    var transaction = db.multi();

    transaction.hset(prefix.hash, this.id, data);
    for (var i = 0; i < scheduler.N; i++) {
      transaction.sadd(prefix.set, i + ':' + this.id);
    }
    this.map.save(this, transaction);
    transaction.exec(callback || function() {});
  };

  Job.prototype.remove = function remove(callback) {
    var transaction = db.multi();

    if (!callback) callback = function() {};

    transaction.hdel(prefix.hash, this.id, data);
    for (var i = 0; i < scheduler.N; i++) {
      transaction.srem(prefix.set, i + ':' + this.id);
    }

    transaction.exec(function() {
      var self = this,
          done = 0;

      async.whilst(function() {
        return done < scheduler.N;
      }, function(callback) {
        db.hget(prefix.replica, done + ':' + self.id, function(err, rand) {
          if (err || !rand) {
            // Skip revreplica deletion and continue erasing
            db.hdel(prefix.replica, done + ':' + self.id, function() {
              callback(null);
            });
            return;
          }
          db.multi()
            .hdel(prefix.replica, done + ':' + self.id)
            .hdel(prefix.revReplica, rand)
            .exec(callback);
        });
      }, callback);
    });
  };

  Job.prototype.enqueue = function enqueue(callback) {
    if (!callback) callback = function() {};
    if (this.index === undefined) {
      return callback(new Error('Job wasn\'t loaded with getJobs'));
    }

    var key = this.index + ':' + (+new Date) + ':' + this.id,
        rand = crypto.randomBytes(32).toString('hex');

    db.multi()
      .sadd(prefix.queue, key)
      .hset(prefix.replica, this.index + ':' + this.id, rand)
      .hset(prefix.revReplica, rand, this.index + ':' + this.id)
      .exec(callback);
  };

  Job.prototype.dequeue = function dequeue(callback) {
    if (!callback) callback = function() {};
    if (this.index === undefined) {
      return callback(new Error('Job wasn\'t loaded with getJobs'));
    }

    var self = this,
        key = this.index + ':' + this.id,
        queueKey = this.index + ':' + this.time + ':' + this.id;

    db.multi()
      .srem(prefix.queue, queueKey)
      .sadd(prefix.set, key)
      .exec(function(err1) {
        db.hget(prefix.replica, key, function(err2, rand) {
          db.multi()
            .hdel(prefix.replica, key)
            .hdel(prefix.revReplica, rand)
            .exec(function(err3) {
              callback(err1 || err2 || err3 || null);
            });
        });
      });
  };

  Job.loadRandom  = function loadRandom(N, callback) {
    if (!callback) callback = function() {};

    var self = this,
        jobs = [];

    async.whilst(function() {
      return jobs.length < N;
    }, function(callback) {
      db.spop(prefix.set, function(err, item) {
        if (err) return callback(err);
        if (!item) return callback(new Error('Empty set'));

        var match = item.match(/^(\d+):(.*)$/),
            index = parseInt(match[1], 10),
            id = match[2];

        Job.load(id, function(err, job) {
          if (err) return callback(err);

          db.hget(prefix.replica, item, function(err, replica) {
            if (err) return callback(err);

            // Retry if job was removed while getting it
            if (job) {
              job.index = index;
              job.replica = replica;
              jobs.push(job);
            }
            callback(null);
          });
        });
      });
    }, function(err) {
      callback(null, jobs);
    });
  };

  // Cleanup loop
  function cleanQueue() {
    var now = +new Date;

    function callback() {
      // And loop after interval
      setTimeout(function() {
        cleanQueue();
      }, scheduler.jobTimeout);
    }

    db.smembers(prefix.queue, function(err, items) {
      if (err) return callback(err);

      // Filter out expired items
      var expired = items.map(function(item) {
        var match = item.match(/^(\d+):(\d+):(.*)$/);
        return {
          index: parseInt(match[1], 10),
          time: parseInt(match[2], 10),
          id: match[3]
        };
      }).filter(function(item) {
        return item.time + scheduler.jobTimeout < now;
      });

      // And return them back
      async.forEach(expired, function(item, callback) {
        Job.prototype.dequeue.call(item, callback);
      }, callback);
    });
  };
  cleanQueue();

  return Job;
};
