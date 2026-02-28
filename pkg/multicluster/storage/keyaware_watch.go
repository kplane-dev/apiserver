package storage

import (
	"k8s.io/apimachinery/pkg/watch"
)

// EntryWatch adapts a watch stream that carries InternalEntry objects into
// a stream that emits only runtime objects to API callers.
type EntryWatch struct {
	source   watch.Interface
	resultCh chan watch.Event
}

func NewEntryWatch(source watch.Interface) watch.Interface {
	if source == nil {
		return nil
	}
	w := &EntryWatch{
		source:   source,
		resultCh: make(chan watch.Event),
	}
	go w.run()
	return w
}

func (w *EntryWatch) Stop() {
	if w.source != nil {
		w.source.Stop()
	}
}

func (w *EntryWatch) ResultChan() <-chan watch.Event {
	return w.resultCh
}

func (w *EntryWatch) run() {
	defer close(w.resultCh)
	for evt := range w.source.ResultChan() {
		entry, ok := evt.Object.(*InternalEntry)
		if !ok || entry == nil {
			w.resultCh <- evt
			continue
		}
		if entry.Object == nil {
			continue
		}
		w.resultCh <- watch.Event{Type: evt.Type, Object: entry.Object}
	}
}

