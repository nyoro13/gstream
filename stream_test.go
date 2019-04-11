package gstream

import (
	"context"
	"testing"
)

func TestNewStream(t *testing.T) {
	org := []interface{}{1, 2, 3, 4, 5, 6}
	ctx := context.Background()
	i := 1
	for r := range NewStream(ctx, org).GetChannel() {
		if r.(int) != i {
			t.Errorf("unexpectd stream value. expected: %d, actual: %d", i, r.(int))
		}
		i++
	}
}

func TestGenerateStream(t *testing.T) {
	genCtx, cancel := context.WithCancel(context.Background())
	cnt := 0
	generator := func() interface{} {
		i := cnt
		cnt++
		if cnt >= 10 {
			cancel()
		}
		return i
	}

	expect := 0
	for r := range GenerateStream(genCtx, generator).GetChannel() {
		if r.(int) != expect {
			t.Errorf("unexpectd stream value. expected: %d, actual: %d", expect, r.(int))
		}
		expect++
	}
}

func TestTake(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())

	expectedLen := 20
	start := 123
	last := 1020
	s := GenerateStream(ctx1, generator(start, last, cancel1)).Take(context.Background(), expectedLen).ToSlice()
	if len(s) == expectedLen {
		cur := start
		for _, i := range s {
			if i != cur {
				t.Errorf("unexpected take value. expected: %d, actual: %d", cur, i)
			}
			cur++
		}
	} else {
		t.Errorf("unexpected take len. expected: %d, actual: %d", expectedLen, len(s))
	}
}

func TestSkip(t *testing.T) {
	skipNum := 239
	start := 249
	last := 2234
	expectedLen := last - start + 1 - skipNum
	if skipNum < 0 {
		expectedLen = last - start + 1
	}

	slice := make([]interface{}, 0)
	for i := start; i <= last; i++ {
		slice = append(slice, i)
	}

	s := NewStream(context.Background(), slice).Skip(context.Background(), skipNum).ToSlice()
	if len(s) == expectedLen {
		cur := start + skipNum
		if skipNum < 0 {
			cur = start
		}
		for _, i := range s {
			if i != cur {
				t.Errorf("unexpected take value. expected: %d, actual: %d", cur, i)
			}
			cur++
		}
	} else {
		t.Errorf("unexpected take len. expected: %d, actual: %d", expectedLen, len(s))
	}
}

func TestMap(t *testing.T) {
	start := 125
	last := 485
	slice := makeSlice(start, last)

	ctx := context.Background()
	stream := NewStream(ctx, slice).Map(ctx, func(i interface{}) interface{} { return i.(int) * i.(int) }).GetChannel()

	for _, i := range slice {
		select {
		case val, ok := <-stream:
			if !ok {
				t.Errorf("unexpected map. stream has already closed. index: %d", i)
				return
			}

			if val != i.(int)*i.(int) {
				t.Errorf("unexpected map value. expected: %d, actual: %d", i.(int)*i.(int), val)
				return
			}
		}
	}
}

func TestFilter(t *testing.T) {
	start := 125
	last := 485
	slice := makeSlice(start, last)

	ctx := context.Background()
	stream := NewStream(ctx, slice).Filter(ctx, func(i interface{}) bool { return i.(int)%2 == 0 })
	for i := range stream.GetChannel() {
		if i.(int)%2 != 0 {
			t.Errorf("unexpected filter value. actual: %d", i)
		}
	}
}

func TestDistinct(t *testing.T) {
	slice := []interface{}{1, 2, 3, 4, 5, 6, 78, 98, 49, 2, 3, 4, 56, 294, 6, 493}
	ctx := context.Background()
	olds := make([]interface{}, 0)
	for i := range NewStream(ctx, slice).Distinct(ctx).GetChannel() {
		for old := range olds {
			if old == i {
				t.Errorf("unexpected distinct value. duplicated value: %d", i)
			}
		}
	}
}

func TestFirst(t *testing.T) {
	cnt := 0
	expected := cnt
	generator := func() interface{} {
		i := cnt
		cnt++
		return i
	}
	val, ok := GenerateStream(context.Background(), generator).First()
	if ok {
		if val != 0 {
			t.Errorf("unexpected first value. expected: %d, actual: %d", expected, val)
		}
	} else {
		t.Errorf("unexpected first value. expected: %d, actual: closed", expected)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	val, ok = GenerateStream(ctx2, generator).First()
	if ok {
		t.Errorf("unexpected first value. expected: closed, actual: %d", val)
	}
}

func generator(min int, max int, cancel func()) func() interface{} {
	cur := min
	return func() interface{} {
		i := cur
		cur++
		if i > max {
			cancel()
		}
		return i
	}
}

func makeSlice(min int, max int) []interface{} {
	slice := make([]interface{}, 0)
	for i := min; i <= max; i++ {
		slice = append(slice, i)
	}
	return slice
}
