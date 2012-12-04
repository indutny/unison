var http = require('http'),
    unison = require('..');

var scheduler = unison.scheduler.create({
  R: 3,
  W: 3,
  jobTimeout: 30000
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

/*
job.index = 0;
for (var i = 0; i < 3; i++) {
  job.addSolution('rand', [1,2,3,4,5], function(err) {
    console.log(err);
  });
}
*/

scheduler.on('complete', function(job, out) {
  console.log('Job %s done:', job.id);
  console.log(out);
});

http.createServer(scheduler.middleware()).listen(8000, function() {
  var addr = this.address();
  console.log('Unison server is listening on %s:%d', addr.address, addr.port);
});
