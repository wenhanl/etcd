package store

import (
	"testing"
)

// TestEventQueue tests a queue with capacity = 100
// Add 200 events into that queue, and test if the
// previous 100 events have been swapped out.
func TestEventQueue(t *testing.T) {

	eh := newEventHistory(100)

	// Add
	for i := 0; i < 200; i++ {
		e := newEvent(Create, "/foo", uint64(i), uint64(i))
		eh.addEvent(e)
	}

	// Test
	j := 100
	i := eh.Queue.Front
	n := eh.Queue.Size
	for ; n > 0; n-- {
		e := eh.Queue.Events[i]
		if e.Index() != uint64(j) {
			t.Fatalf("queue error!")
		}
		j++
		i = (i + 1) % eh.Queue.Capacity
	}
}

func TestScanHistory(t *testing.T) {
	eh := newEventHistory(100)

	// Add
	eh.addEvent(newEvent(Create, "/foo", 1, 1))
	eh.addEvent(newEvent(Create, "/foo/bar", 2, 2))
	eh.addEvent(newEvent(Create, "/foo/foo", 3, 3))
	eh.addEvent(newEvent(Create, "/foo/bar/bar", 4, 4))
	eh.addEvent(newEvent(Create, "/foo/foo/foo", 5, 5))

	// Set some key
	eh.addEvent(newEvent(Set, "/foo", 6, 6))
	eh.addEvent(newEvent(Set, "/foo/foo", 7, 7))

	//init test slice
	var valid = true

	e, err := eh.scan("/foo", false, 1)
	valid = e[0].Index() == 1 && e[1].Index() == 6
	if err != nil || !valid {
		t.Fatalf("scan error [/foo] [1] %v", e[1].Index())
	}

	e, err = eh.scan("/foo/foo", false, 1)
	valid = e[0].Index() == 3 && e[1].Index() == 7
	if err != nil || !valid {
		t.Fatalf("scan error [/foo/foo] [2] %v", e[0].Index())
	}

	e, err = eh.scan("/foo/bar", false, 1)
	valid = e[0].Index() == 2
	if err != nil || !valid {
		t.Fatalf("scan error [/foo/bar] [2] %v", e[0].Index())
	}

	e, err = eh.scan("/foo/foo", true, 1)
	valid = e[0].Index() == 3 && e[1].Index() == 5 && e[2].Index() == 7
	if err != nil || !valid {
		t.Fatalf("scan error [/foo/foo] [1] recurisive %v", e[0].Index())
	}

	e, err = eh.scan("/foo/bar", true, 8)

	if e != nil {
		t.Fatalf("bad index shoud reuturn nil")
	}
}

// TestFullEventQueue tests a queue with capacity = 10
// Add 1000 events into that queue, and test if scanning
// works still for previous events.
func TestFullEventQueue(t *testing.T) {

	eh := newEventHistory(10)

	// Add
	for i := 0; i < 1000; i++ {
		e := newEvent(Create, "/foo", uint64(i), uint64(i))
		eh.addEvent(e)
		events, err := eh.scan("/foo", true, uint64(i-1))
		if len(events) != 0 {
			e = events[0]
		}
		if i > 0 {
			if e == nil || err != nil {
				t.Fatalf("scan error [/foo] [%v] %v", i-1, i)
			}
		}
	}
}
