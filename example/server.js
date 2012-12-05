var http = require('http'),
    cluster = require('cluster'),
    unison = require('..');

if (cluster.isMaster) {
  for (var i = 0; i < 3; i++) {
    cluster.fork();
  }
}

var scheduler = unison.scheduler.create({
  R: 10,
  W: 10,
  jobTimeout: 3000
});

var job = scheduler.job.create({
  id: 'test',
  input: [ 0, 1, 2, 3 ],
  map: function(data) {
    return data.map(function(value) {
      return value + 1;
    });
  },
  continuous: true
});
scheduler.add(job);

scheduler.on('complete', function(job, out) {
  if (cluster.isMaster) {
    console.log('Job %s done:', job);
    console.log(out);
  }
});

http.createServer(
  scheduler.middleware()
).listen(8000, function() {
  var addr = this.address();
  console.log('Unison server is listening on %s:%d', addr.address, addr.port);

  // Just one line!
  unison.client.connect('http://localhost:8000/unison');
});
