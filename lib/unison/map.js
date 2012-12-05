// Model start
exports.create = function create(scheduler) {

var async = require('async'),
    crypto = require('crypto'),
    uglifyjs = require('uglify-js');

// Some common db prefixes
var db = scheduler.db,
    prefix = {
      hash: db.prefix + 'm/h',
      set: db.prefix + 'm/s/'
    };

//
// ### function Map (options)
// #### @options {Object|String} instance data
// Map constructor.
//
function Map(options) {
  this.source = typeof options === 'object' ? options.source :
                                              options.toString();
  this.id = typeof options === 'object' && options.id ||
            crypto.createHash('sha1').update(this.source).digest('hex');

  this.uglify();
};
Map.create = function create(options) {
  return new Map(options);
};

Map.prototype.uglify = function uglify() {
  if (!this.source) return;

  var ast = uglifyjs.parse('(' + this.source + ')');
  ast.figure_out_scope();

  var compressor = uglifyjs.Compressor({
    dead_code: false,
    side_effects: false
  });
  ast = ast.transform(compressor);

  ast.figure_out_scope();
  ast.compute_char_frequency();
  ast.mangle_names();
  this.source = ast.print_to_string().slice(1, -2);
};

//
// ### function toJSON ()
// Convert instance to JSON
//
Map.prototype.toJSON = function toJSON() {
  return this.source;
};

//
// ### function load (id, callback)
// #### @id {String} Map id to load
// #### @callback {Function} *optional* Continuation
// Load map from database
//
Map.load = function load(id, callback) {
  if (!callback) callback = function() {};

  db.hget(prefix.hash, id, function(err, map) {
    if (err) return callback(err);
    if (!map) return callback(new Error('Map not found'));

    callback(null, Map.create(JSON.parse(map)));
  });
};

//
// ### function save (job, transaction, callback)
// #### @job {Job} job to associate map with
// #### @transaction {db.Multi} *optional* transaction to append
// #### @callback {Function} Continuation
// Save map in database
//
Map.prototype.save = function save(job, transaction, callback) {
  if (typeof transaction === 'function') {
    callback = transaction;
    transaction = this.db.multi();
  }

  var data = JSON.stringify(this.toJSON());
  if (callback) {
    transaction.hset(prefix.hash, this.id, data)
               .sadd(prefix.set + this.id, job.id || job)
               .exec(callback);
  } else {
    // We're reusing existing transaction, no need to execute it here
    transaction.hset(prefix.hash, this.id, data)
               .sadd(prefix.set + this.id, (job.id || job));
  }
};

return Map;

// Model end
};
