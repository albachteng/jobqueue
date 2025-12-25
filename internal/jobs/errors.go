package jobs

/*
DO NOT: type JobError[T any] struct { ... }
Errors should be inspectable with error.Is()
Generics add no value
*/

// TODO: implement job-specific errors
