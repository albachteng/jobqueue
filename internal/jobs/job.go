type Job[T any] struct {
    ID        JobID
    Payload   T
    CreatedAt time.Time
}
