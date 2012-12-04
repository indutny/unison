# Unison

## Example

```javascript
var unison = require('unison');

var scheduler = unison.scheduler.create({
  N: 5,
  R: 3,
  W: 3
});

var job = unison.job.create({
  id: /* unique job id */,
  input: /* some data blob */,
  map: /* map function, either source string or function */,
  continuous: true // if true - job's input will be replaced by it's output on
                   // computation completion, and job will be immediately
                   // reenqueued again
});

// Enqueue job
scheduler.add(job);

// Dequeue job
scheduler.remove(job.id); // or scheduler.remove(job)

// This event will be emitted upon job completion
job.on('complete', function(out) {
});

// And you can use this scheduler in your middleware
app.use(scheduler.middleware());
```
