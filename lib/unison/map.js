var async = require('async'),
    crypto = require('crypto');

exports.create = function create(scheduler) {
  var db = scheduler.db,
      prefix = {
        hash: db.prefix + 'm:h',
        set: db.prefix + 'm:s'
      };

  function Map(options) {
    this.source = typeof options === 'object' ? options.source :
                                                options.toString();
    this.id = typeof options === 'object' && options.id ||
              crypto.createHash('sha1').update(this.source).digest('hex');
  };
  Map.create = function create(options) {
    return new Map(options);
  };

  Map.prototype.toJSON = function toJSON() {
    return this.source;
  };

  Map.load = function load(id, callback) {
    db.hget(prefix.hash, id, function(err, map) {
      if (err) return callback(err);
      if (!map) return callback(new Error('Map not found'));

      callback(null, Map.create(JSON.parse(map)));
    });
  };

  Map.prototype.save = function save(job, transaction, callback) {
    if (typeof transaction === 'function') {
      callback = transaction;
      transaction = this.db.multi();
    }

    var data = JSON.stringify(this.toJSON());
    if (callback) {
      transaction.hset(prefix.hash, this.id, data)
                 .sadd(prefix.set, this.id, job.id || job)
                 .exec(callback);
    } else {
      transaction.hset(prefix.hash, this.id, data)
                 .sadd(prefix.set, this.id, this.id + ':' + (job.id || job));
    }
  };

  return Map;
};
