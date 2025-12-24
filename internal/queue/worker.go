package queue

/*
DO NOT: type Worker[T any] struct { ... }
execute behavior, not types
workers often deal with side-effects
best expressed with interfaces
generics can go inside the envelope, as below:
*/

// TODO: implement Handler interface when JobEnvelope is defined
// type Handler interface {
// 	Handle(ctx context.Context, job JobEnvelope) error
// }
