var http = require('http'),
    unison = require('..');

var scheduler = unison.scheduler.create({
  N: 5,
  R: 3,
  W: 3
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

job.on('complete', function(out) {
  console.log('Job done', out);
});

http.createServer(scheduler.middleware()).listen(8000, function() {
  var addr = this.address();
  console.log('Unison server is listening on %s:%d', addr.address, addr.port);
});
