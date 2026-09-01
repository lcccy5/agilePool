package agilepool

import "sync"

type taskBufferPushResult int

const (
	taskBufferAccepted taskBufferPushResult = iota
	taskBufferClosed
	taskBufferFull
)

const (
	// maxWorkerTaskBatchSize bounds how long one worker can drain the overflow
	// queue before returning to the handoff channel. Keeping it bounded avoids
	// turning high backlog throughput into unbounded latency for newer tasks.
	maxWorkerTaskBatchSize = 64
)

// taskChunk is a fixed-size node in the linked-list task buffer.
// Each chunk holds up to taskChunkSize Task values. Using small,
// fixed-size nodes avoids the doubling overhead of a single slice
// and makes GC work with smaller, independently collectable objects.
type taskChunk struct {
	tasks [taskChunkSize]Task
	next  *taskChunk
}

// chunkedTaskBuffer stores overflow tasks in fixed-size linked chunks.
// It owns the buffer lock and recycles consumed chunks through sync.Pool.
type chunkedTaskBuffer struct {
	headChunk      *taskChunk // head of linked-list buffer (where workers pop)
	tailChunk      *taskChunk // tail of linked-list buffer (where submitters push)
	headIdx        int        // pop index within headChunk
	tailIdx        int        // push index within tailChunk
	chunkLen       int64      // total tasks across all chunks (replaces len(taskBuf))
	taskMu         sync.Mutex // protects the chunked buffer
	overflowClosed bool       // prevents spool after Close
	chunkPool      sync.Pool  // recycles consumed taskChunk nodes
}

func newChunkedTaskBuffer() *chunkedTaskBuffer {
	b := &chunkedTaskBuffer{}
	b.chunkPool.New = func() interface{} {
		return &taskChunk{}
	}
	return b
}

func (b *chunkedTaskBuffer) Len() int64 {
	b.taskMu.Lock()
	defer b.taskMu.Unlock()

	return b.chunkLen
}

func (b *chunkedTaskBuffer) Close() {
	b.taskMu.Lock()
	defer b.taskMu.Unlock()

	b.overflowClosed = true
}

// PushAndForward appends task to the buffer, then tries to forward one
// buffered task through tryForward. If forwarding fails, the task is pushed
// back to the buffer tail.
func (b *chunkedTaskBuffer) PushAndForward(task Task, tryForward func(Task) bool) taskBufferPushResult {
	b.taskMu.Lock()
	defer b.taskMu.Unlock()

	if b.overflowClosed {
		return taskBufferClosed
	}
	if b.chunkLen >= maxChunkLen {
		return taskBufferFull
	}

	b.pushTail(task)

	if t, ok := b.popHead(); ok {
		if !tryForward(t) {
			b.pushTail(t)
		}
	}

	return taskBufferAccepted
}

func (b *chunkedTaskBuffer) PopBatch(dst []Task) int {
	b.taskMu.Lock()
	defer b.taskMu.Unlock()

	n := 0
	for n < len(dst) {
		t, ok := b.popHead()
		if !ok {
			break
		}
		dst[n] = t
		n++
	}

	return n
}

// PopAdaptiveBatch drains a backlog-dependent number of tasks into dst.
// The batch size is selected while taskMu is held, so workers do not take a
// separate length snapshot lock before draining. Shallow queues use tiny
// batches for fairness; deeper queues amortize the shared lock acquisition.
func (b *chunkedTaskBuffer) PopAdaptiveBatch(dst []Task) int {
	b.taskMu.Lock()
	defer b.taskMu.Unlock()

	limit := adaptiveTaskBatchSize(b.chunkLen, len(dst))
	n := 0
	for n < limit {
		t, ok := b.popHead()
		if !ok {
			break
		}
		dst[n] = t
		n++
	}

	return n
}

// adaptiveTaskBatchSize balances queue fairness and lock amortization.
// maxSize is normally the worker's stack batch capacity; zero means there is
// no destination space and therefore no work should be removed.
func adaptiveTaskBatchSize(backlog int64, maxSize int) int {
	if backlog <= 0 || maxSize <= 0 {
		return 0
	}

	var size int
	switch {
	case backlog <= 8:
		size = 1
	case backlog <= 64:
		size = 8
	case backlog <= 512:
		size = 32
	default:
		size = maxWorkerTaskBatchSize
	}

	if size > maxSize {
		return maxSize
	}
	return size
}

// pushTail appends a task to the tail of the chunked buffer.
// Must be called with taskMu held.
func (b *chunkedTaskBuffer) pushTail(task Task) {
	if b.tailChunk == nil {
		c := b.chunkPool.Get().(*taskChunk)
		b.tailChunk = c
		b.headChunk = c
		b.tailIdx = 0
		b.headIdx = 0
	} else if b.tailIdx >= taskChunkSize {
		c := b.chunkPool.Get().(*taskChunk)
		b.tailChunk.next = c
		b.tailChunk = c
		b.tailIdx = 0
	}

	b.tailChunk.tasks[b.tailIdx] = task
	b.tailIdx++
	b.chunkLen++
}

// popHead removes and returns the task at the head of the chunked buffer.
// Returns nil, false if the buffer is empty.
// Must be called with taskMu held.
func (b *chunkedTaskBuffer) popHead() (Task, bool) {
	if b.headChunk == nil {
		return nil, false
	}
	if b.headIdx >= taskChunkSize {
		// Advance to next chunk; recycle the exhausted one.
		next := b.headChunk.next
		b.headChunk.next = nil
		b.chunkPool.Put(b.headChunk)
		b.headChunk = next
		b.headIdx = 0
		if b.headChunk == nil {
			b.tailChunk = nil
			b.tailIdx = 0
			return nil, false
		}
	}

	// Head caught up to tail in the same chunk - queue drained.
	if b.headChunk == b.tailChunk && b.headIdx >= b.tailIdx {
		b.headChunk.next = nil
		b.chunkPool.Put(b.headChunk)
		b.headChunk = nil
		b.tailChunk = nil
		b.headIdx = 0
		b.tailIdx = 0
		return nil, false
	}

	task := b.headChunk.tasks[b.headIdx]
	b.headChunk.tasks[b.headIdx] = nil // help GC
	b.headIdx++
	b.chunkLen--
	return task, true
}
