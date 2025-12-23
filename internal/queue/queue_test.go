func TestQueue_Enqueue(t *testing.T) {
    tests := []struct {
        name string
        job  Job[string]
        want error
    }{
        {"ok", Job[string]{Payload: "x"}, nil},
        {"context canceled", ..., context.Canceled},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            ...
        })
    }
}
