package benchmark_test

import (
	"testing"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// prepareOverflowBacklog blocks the only worker, then fills the one-slot
// handoff channel plus exactly backlog tasks in the overflow buffer. Releasing
// the returned channel starts a reproducible overflow-drain scenario.
func prepareOverflowBacklog(tb testing.TB, backlog int) (*agilepool.Pool, chan struct{}) {
	tb.Helper()

	pool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(1),
		agilepool.WithTaskQueueSize(1),
	))
	started := make(chan struct{})
	release := make(chan struct{})
	pool.Submit(agilepool.TaskFunc(func() error {
		close(started)
		<-release
		return nil
	}))
	<-started

	noop := agilepool.TaskFunc(func() error { return nil })
	for i := 0; i <= backlog; i++ {
		pool.Submit(noop)
	}

	return pool, release
}

// BenchmarkAgilePoolAdaptiveBatchDrain compares end-to-end overflow draining
// at the four adaptive batching thresholds. It measures the actual public pool
// lifecycle, so results include queueing, worker scheduling, and task drain.
func BenchmarkAgilePoolAdaptiveBatchDrain(b *testing.B) {
	tests := []struct {
		name    string
		backlog int
	}{
		{name: "backlog_1_batch_1", backlog: 1},
		{name: "backlog_9_batch_8", backlog: 9},
		{name: "backlog_65_batch_32", backlog: 65},
		{name: "backlog_513_batch_64", backlog: 513},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pool, release := prepareOverflowBacklog(b, tt.backlog)
				close(release)
				pool.Wait()
				pool.Close()
			}
			b.ReportMetric(float64(tt.backlog), "buffer-tasks/op")
		})
	}
}
