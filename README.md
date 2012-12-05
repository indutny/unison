# Unison

Engine for distributed computation in untrusted environment.

## Example

Server:
```javascript
var unison = require('unison');

var scheduler = unison.scheduler.create({
  prefix: '/unison',
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
job.on('complete', function(job, out) {
});

// And you can use this scheduler in your middleware
app.use(scheduler.middleware());
```

Client:
```javascript
unison.client.connect('http://server/unison');
```

#### LICENSE

This software is licensed under the MIT License.

Copyright Fedor Indutny, 2012.

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the
following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
USE OR OTHER DEALINGS IN THE SOFTWARE.
