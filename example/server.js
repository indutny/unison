var http = require('http'),
    unison = require('..');

var scheduler = unison.scheduler.create({
  R: 3,
  W: 3,
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
  console.log('Job %s done:', job);
  console.log(out);
});

http.createServer(
  scheduler.middleware()
).listen(8000, function() {
  var addr = this.address();
  console.log('Unison server is listening on %s:%d', addr.address, addr.port);

  // Just one line!
  unison.client.connect('http://localhost:8000/unison');
});
