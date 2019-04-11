package gstream

import (
	"context"
	"runtime"
)

type Stream struct {
	stream <-chan interface{}
	isOpen bool
	done   chan<- interface{}
}

var bufSize = runtime.NumCPU()

func newStream(stream <-chan interface{}, done chan interface{}) *Stream {
	return &Stream{stream: stream, isOpen: false, done: done}
}

func execDefault(ctx context.Context, done <-chan interface{}, reader <-chan interface{}, fn func(interface{})) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case val, ok := <-reader:
			if !ok {
				return
			}
			fn(val)
		}
	}
}

func NewStream(ctx context.Context, slice []interface{}) *Stream {
	stream := make(chan interface{}, bufSize)
	done := make(chan interface{})
	go func() {
		defer close(stream)
		for _, i := range slice {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case stream <- i:
			}
		}
	}()

	return newStream(stream, done)
}

func GenerateStream(ctx context.Context, generator func() interface{}) *Stream {
	stream := make(chan interface{}, bufSize)
	done := make(chan interface{})

	go func() {
		defer close(stream)
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case stream <- generator():
			}
		}
	}()

	return newStream(stream, done)
}

func (s *Stream) open() <-chan interface{} {
	if s.isOpen {
		panic("stream has already opened.")
	}

	return s.stream
}

func (s *Stream) GetChannel() <-chan interface{} {
	return s.open()
}

func (s *Stream) Take(ctx context.Context, num int) *Stream {
	stream := make(chan interface{}, num)
	done := make(chan interface{})

	go func() {
		defer close(stream)
		cnt := 0
		reader := s.open()
		for {
			if cnt >= num {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case val, ok := <-reader:
				if !ok {
					return
				}
				stream <- val
				cnt++
			}
		}
	}()

	return newStream(stream, done)
}

func (s *Stream) Skip(ctx context.Context, num int) *Stream {
	stream := make(chan interface{}, bufSize)
	done := make(chan interface{})

	go func() {
		defer close(stream)
		curCnt := 0
		execDefault(ctx, done, s.open(), func(i interface{}) {
			if curCnt >= num {
				stream <- i
			} else {
				curCnt++
			}
		})
	}()

	return newStream(stream, done)
}

func (s *Stream) Map(ctx context.Context, mapFunc func(interface{}) interface{}) *Stream {
	stream := make(chan interface{}, bufSize)
	done := make(chan interface{})

	go func() {
		defer close(stream)
		execDefault(ctx, done, s.open(), func(i interface{}) { stream <- mapFunc(i) })
	}()
	return newStream(stream, done)
}

func (s *Stream) Filter(ctx context.Context, filter func(val interface{}) bool) *Stream {
	stream := make(chan interface{}, bufSize)
	done := make(chan interface{})

	go func() {
		defer close(stream)
		execDefault(ctx, done, s.open(), func(i interface{}) {
			if filter(i) {
				stream <- i
			}
		})
	}()
	return newStream(stream, done)
}

func (s *Stream) Distinct(ctx context.Context) *Stream {
	stream := make(chan interface{}, bufSize)
	done := make(chan interface{})

	go func() {
		defer close(stream)
		olds := make([]interface{}, 0)
		execDefault(ctx, done, s.open(), func(i interface{}) {
			for _, old := range olds {
				if old == i {
					return
				}
			}
			olds = append(olds, i)
			stream <- i
		})
	}()
	return newStream(stream, done)
}

func (s *Stream) First() (interface{}, bool) {
	defer close(s.done)
	select {
	case i, ok := <-s.open():
		return i, ok
	}
}

func (s *Stream) ToSlice() []interface{} {
	defer close(s.done)
	slice := make([]interface{}, 0)
	for val := range s.open() {
		slice = append(slice, val)
	}
	return slice
}
