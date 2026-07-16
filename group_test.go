package concurrency

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

const groupTestTimeout = time.Second

func TestGroupRejectsInvalidLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		t.Run("limit", func(t *testing.T) {
			group, err := NewGroup(context.Background(), limit)
			if group != nil || !errors.Is(err, ErrInvalidLimit) {
				t.Fatalf("NewGroup(limit=%d)=(%v,%v), want (nil, %v)", limit, group, err, ErrInvalidLimit)
			}
		})
	}
}

func TestGroupRejectsNilFunction(t *testing.T) {
	group, err := NewGroup(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := group.Go(nil); !errors.Is(err, ErrNilFunction) {
		t.Fatalf("Go(nil)=%v, want %v", err, ErrNilFunction)
	}
	if err := group.Wait(); err != nil {
		t.Fatalf("Wait()=%v, want nil", err)
	}
}

func TestGroupBoundAndClosure(t *testing.T) {
	group, err := NewGroup(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	started := make(chan struct{}, 2)
	var running atomic.Int32
	var peak atomic.Int32
	work := func(context.Context) error {
		current := running.Add(1)
		defer running.Add(-1)
		for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
		}
		started <- struct{}{}
		<-release
		return nil
	}

	for range 2 {
		if err := group.Go(work); err != nil {
			t.Fatal(err)
		}
	}
	waitForGroupSignal(t, started)
	waitForGroupSignal(t, started)

	thirdSubmission := make(chan error, 1)
	go func() {
		thirdSubmission <- group.Go(func(context.Context) error { return nil })
	}()
	select {
	case err := <-thirdSubmission:
		t.Fatalf("third Go returned before capacity was available: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-thirdSubmission:
		if err != nil {
			t.Fatalf("third Go=%v, want nil", err)
		}
	case <-time.After(groupTestTimeout):
		t.Fatal("third Go did not acquire released capacity")
	}

	if err := group.Wait(); err != nil {
		t.Fatalf("Wait()=%v, want nil", err)
	}
	if got := peak.Load(); got != 2 {
		t.Fatalf("peak concurrency=%d, want 2", got)
	}
	if err := group.Go(func(context.Context) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("late Go=%v, want %v", err, ErrClosed)
	}
}

func TestGroupJoinsErrorsBySubmissionIndex(t *testing.T) {
	group, err := NewGroup(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}

	wants := []error{errors.New("first"), errors.New("second"), errors.New("third")}
	releases := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	completed := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	for index := range wants {
		index := index
		if err := group.Go(func(context.Context) error {
			<-releases[index]
			close(completed[index])
			return wants[index]
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, index := range []int{2, 1, 0} {
		close(releases[index])
		waitForGroupSignal(t, completed[index])
	}

	err = group.Wait()
	for _, want := range wants {
		if !errors.Is(err, want) {
			t.Fatalf("Wait error=%v, want errors.Is(_, %v)", err, want)
		}
	}
	if got, want := err.Error(), "first\nsecond\nthird"; got != want {
		t.Fatalf("Wait error=%q, want %q", got, want)
	}
}

func TestGroupCallbackErrorCancelsPeersWithCause(t *testing.T) {
	group, err := NewGroup(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}

	want := errors.New("first callback failed")
	peerStarted := make(chan struct{})
	peerCause := make(chan error, 1)
	if err := group.Go(func(context.Context) error {
		<-peerStarted
		return want
	}); err != nil {
		t.Fatal(err)
	}
	if err := group.Go(func(ctx context.Context) error {
		close(peerStarted)
		<-ctx.Done()
		peerCause <- context.Cause(ctx)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := group.Wait(); !errors.Is(err, want) {
		t.Fatalf("Wait()=%v, want %v", err, want)
	}
	select {
	case cause := <-peerCause:
		if !errors.Is(cause, want) {
			t.Fatalf("peer context cause=%v, want %v", cause, want)
		}
	case <-time.After(groupTestTimeout):
		t.Fatal("peer did not observe callback failure")
	}
}

func TestGroupGoReturnsParentCancellationCause(t *testing.T) {
	parent, cancel := context.WithCancelCause(context.Background())
	group, err := NewGroup(parent, 1)
	if err != nil {
		t.Fatal(err)
	}

	want := errors.New("parent stopped")
	cancel(want)
	var called atomic.Bool
	if err := group.Go(func(context.Context) error {
		called.Store(true)
		return nil
	}); !errors.Is(err, want) {
		t.Fatalf("Go()=%v, want %v", err, want)
	}
	if called.Load() {
		t.Fatal("callback ran after parent cancellation")
	}
}

func waitForGroupSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(groupTestTimeout):
		t.Fatal("timed out waiting for group signal")
	}
}
