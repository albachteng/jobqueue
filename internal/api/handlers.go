package api

type Result[T any] struct {
	Value T
	Err   error
}
