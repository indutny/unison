var async = require('async'),
    crypto = require('crypto'),
    events = require('events'),
    util = require('util'),
    unison = require('../unison'),
    EventEmitter = events.EventEmitter;

exports.create = function create(scheduler) {
  var db = scheduler.db,
      prefix = {
        hash: db.prefix + 'j/h',
        set: db.prefix + 'j/s',
        queue: db.prefix + 'j/q',
        replica: db.prefix + 'j/r',
        revReplica: db.prefix + 'j/rr',
        solutionsRank: db.prefix + 'j/sr/',
        solutionsHash: db.prefix + 'j/sh/',
        complete: db.prefix + 'j/c/',
        completeR: new RegExp('^' + db.prefix.replace(/([^\w\d])/g, '\\$1') +
                              'j\\/c\\/(.*)$')
      };

  function Job(options) {
    EventEmitter.call(this);

    this.id = options.id;
    this.input = options.input;
    this.map = scheduler.map.create(options.map || '');
    this.continuous = options.continuous === true;

    // Index of job replica
    this.index = options.index;

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

  Job.prototype.queueId = function queueId() {
    return this.iteration + ':' + this.id;
  };

  Job.parseId = function parseId(id) {
    var match = id.match(/^(\d+):(.*)$/);
    if (match === null) return { id: id, iteration: null };

    return {
      iteration: parseInt(match[1], 10),
      id: match[2]
    };
  };

  Job.load = function load(job, callback) {
    if (!callback) callback = function() {};

    var self = this,
        parsedId = Job.parseId(job.id || job),
        id = parsedId.id;

    db.hget(prefix.hash, id, function(err, job) {
      if (err || !job) return callback(new Error('not found'), null);

      job = Job.create(JSON.parse(job));
      if (parsedId.iteration === null || parsedId.iteration === job.iteration) {
        callback(null, job);
      } else {
        callback(new Error('Iteration mistmatch'));
      }
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
      transaction.sadd(prefix.set, i + ':' + this.queueId());
    }
    this.map.save(this, transaction);
    transaction.exec(callback || function() {});
  };

  Job.prototype.remove = function remove(dequeue, callback) {
    var self = this,
        transaction = db.multi();

    if (typeof dequeue === 'callback') {
      callback = dequeue;
      dequeue = false;
    }

    if (!callback) callback = function() {};

    if (!dequeue) transaction.hdel(prefix.hash, this.id, data);
    for (var i = 0; i < scheduler.N; i++) {
      transaction.srem(prefix.set, i + ':' + this.queueId());
    }

    transaction.del(prefix.solutionsRank + this.queueId());
    transaction.del(prefix.solutionsHash + this.queueId());
    transaction.exec(function() {
      var done = 0;

      async.whilst(function() {
        return done < scheduler.N;
      }, function(callback) {
        db.hget(prefix.replica,
                done + ':' + self.queueId(),
                function(err, rand) {
          if (err || !rand) {
            // Skip revreplica deletion and continue erasing
            db.hdel(prefix.replica, done + ':' + self.queueId(), function() {
              done++;
              callback(null);
            });
            return;
          }
          db.multi()
            .hdel(prefix.replica, done + ':' + self.queueId())
            .hdel(prefix.revReplica, rand)
            .exec(callback);
          done++;
        });
      }, callback);
    });
  };

  Job.prototype.enqueue = function enqueue(callback) {
    if (!callback) callback = function() {};
    if (this.index === undefined) {
      return callback(new Error('Job wasn\'t loaded with getJobs'));
    }

    var key = this.index + ':' + this.queueId(),
        rand = crypto.randomBytes(32).toString('hex');

    db.multi()
      .zadd(prefix.queue, +new Date, key)
      .hset(prefix.replica, this.index + ':' + this.queueId(), rand)
      .hset(prefix.revReplica, rand, this.index + ':' + this.queueId())
      .exec(callback);

    return rand;
  };

  Job.prototype.dequeue = function dequeue(force, callback) {
    if (typeof force === 'function') {
      callback = force;
      force = false;
    }
    if (!callback) callback = function() {};
    if (this.index === undefined) {
      return callback(new Error('Job wasn\'t loaded with getJobs'));
    }

    var self = this,
        key = this.index + ':' + this.queueId();

    var transaction = db.multi();
    transaction.zrem(prefix.queue, key);
    if (!force) transaction.sadd(prefix.set, key);
    transaction.exec(function(err1) {
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

  Job.prototype.addSolution = function addSolution(solution, callback) {
    if (!callback) callback = function() {};

    if (!solution) {
      return callback(new Error('Solution is incorrect'));
    }

    var self = this;
    this.dequeue(true, function(err) {
      if (err) return callback(err);

      var out = JSON.stringify(solution),
          hash = crypto.createHash('sha1').update(out).digest('hex'),
          key = prefix.solutionsRank + self.queueId();

      db.multi()
        .zincrby(key, 1, hash)
        .hset(prefix.solutionsHash + self.queueId(), hash, out)
        .zrevrangebyscore(key,
                          'inf',
                          0,
                          'WITHSCORES')
        .exec(function(err, results) {
          if (err) return callback(err);

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

          // If the most of the people are lying or if positive result
          // is equal to R value - use it as output
          if (total >= scheduler.N || max >= scheduler.R) {
            // Notify everyone about result, and remove job from queue
            db.hget(prefix.solutionsHash + self.queueId(),
                    maxKey,
                    function(err, o) {
              self.remove(true, function() {
                db.publish(prefix.complete + self.queueId(), o || out);
                callback(null, true);
              });
            });
          } else {
            callback(null, false);
          }
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

          // Retry if job was removed while getting it
          if (job) {
            job.index = index;
            jobs.push(job);
          }
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

    Job.load(id, function(err, job) {
      if (err) return;

      scheduler.emit('complete', job, out);
    });
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
    db.zrangebyscore(prefix.queue, 0, end, function(err, items) {
      if (err) return callback(err);

      // Filter out expired items
      var expired = items.map(function(item) {
        var match = item.match(/^(\d+):(.*)$/);
        return {
          index: parseInt(match[1], 10),
          id: match[2]
        };
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
