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
        replica: db.prefix + 'j/r/',
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

  Job.loadReplica = function loadReplica(repid, callback) {
    db.hget(prefix.revReplica, repid, function(err, id) {
      if (err) return callback(err);
      if (!id) return callback(new Error('Replica not found'));

      Job.load(id, callback);
    });
  };

  Job.prototype.save = function save(callback) {
    var data = JSON.stringify(this.toJSON());

    var transaction = db.multi();

    transaction.hset(prefix.hash, this.id, data);
    transaction.sadd(prefix.queue, this.queueId());

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
    transaction.srem(prefix.queue, this.queueId());
    transaction.zrem(prefix.set, this.queueId());
    transaction.del(prefix.solutionsRank + this.queueId());
    transaction.del(prefix.solutionsHash + this.queueId());
    transaction.exec(function() {
      db.smembers(prefix.replica + self.queueId(), function(err, members) {
        if (err) return callback(null);

        async.forEach(members, function(repid, callback) {
          db.hdel(prefix.revReplica, repid, callback);
        }, function() {
          db.del(prefix.replica + self.queueId(), function() {
            callback(null);
          });
        });
      });
    });
  };

  Job.prototype.enqueue = function enqueue(callback) {
    if (!callback) callback = function() {};

    var key = this.queueId(),
        rand = crypto.randomBytes(32).toString('hex');

    db.multi()
      .zadd(prefix.set, +new Date, rand + ':' + key)
      .sadd(prefix.replica + key, rand)
      .hset(prefix.revReplica, rand, key)
      .exec(callback);

    return rand;
  };

  Job.prototype.dequeue = function dequeue(rand, callback) {
    if (!callback) callback = function() {};

    var self = this,
        key = Job.prototype.queueId.call(this);

    db.multi()
      .srem(prefix.replica + key, rand)
      .hdel(prefix.revReplica, rand)
      .exec(callback);
  };

  Job.prototype.addSolution = function addSolution(repid, solution, callback) {
    if (!callback) callback = function() {};

    if (!solution) {
      return callback(new Error('Solution is incorrect'));
    }

    var self = this;
    this.dequeue(repid, function(err) {
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
      db.srandmember(prefix.queue, function(err, item) {
        if (err) return callback(err);
        if (!item) return callback(new Error('Empty set'));

        var match = item.match(/^(\d+):(.*)$/),
            index = parseInt(match[1], 10),
            id = match[2];

        Job.load(id, function(err, job) {
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
    db.zrangebyscore(prefix.set, 0, end, function(err, items) {
      if (err) return callback(err);

      // Filter out expired items
      var expired = items.map(function(id) {
        var match = id.match(/^([\w\d]+):(\d+):(.*)$/);

        return {
          all: id,
          rand: match[1],
          iteration: parseInt(match[2], 10),
          id: match[3]
        };
      });

      // And return them back
      async.forEach(expired, function(item, callback) {
        db.zrem(prefix.set, item.all, function() {
          Job.prototype.dequeue.call(item, item.rand, callback);
        });
      }, callback);
    });
  };
  cleanQueue();

  return Job;
};
