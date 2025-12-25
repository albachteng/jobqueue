package api

/*
DO NOT: func Handler[T any](...) http.HandlerFunc
HTTP barriers should be concrete, never generic
net/http is already interface-driven
doing the above gains you nothing and loses readability
debugging becomes painful
*/

// TODO: implement HTTP handlers
