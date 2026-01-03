package jobs

import "errors"

/*
DO NOT: type JobError[T any] struct { ... }
Errors should be inspectable with error.Is()
Generics add no value
*/

var ErrJobFailed = errors.New("job failed")
