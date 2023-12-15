// Copyright 2023 Ailn(ailnindex@qq.com). All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package hourglass provide a timing wheel for go
package hourglass

import (
	"context"
	"errors"
	"sync"
	"time"
)

var hourglass = NewHourglass(time.Second, 60)

// Hourglass is a timing wheel
type Hourglass struct {
	tickDuration time.Duration
	cancel       context.CancelFunc
	rootWheel    *wheel
}

// NewHourglass make an hourglass with tickDuration and ticksPerWheel
func NewHourglass(tickDuration time.Duration, ticksPerWheel int) *Hourglass {
	ctx, cancel := context.WithCancel(context.TODO())
	hourglass := Hourglass{
		tickDuration: tickDuration,
		cancel:       cancel,
		rootWheel: &wheel{
			currentTick:   0,
			tickDuration:  tickDuration,
			ticksPerWheel: ticksPerWheel,
			tickers:       make([]map[*Ticker]struct{}, ticksPerWheel),
			locker:        sync.Mutex{},
		},
	}
	go hourglass.run(ctx)
	return &hourglass
}

// Close hourglass
func (h *Hourglass) Close() {
	h.cancel()
}

func (h *Hourglass) run(ctx context.Context) {
	ticker := time.NewTicker(h.tickDuration)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case t := <-ticker.C:
			h.rootWheel.tick(t)
		}
	}
}

// Ticker provide a time ticker like time.Ticker
type Ticker struct {
	duration time.Duration
	arg      any
	f        func(arg any, t time.Time)
	closed   bool
	locker   sync.RWMutex
}

// C is channel of time.Time
func (t *Ticker) C() <-chan time.Time {
	return t.arg.(chan time.Time)
}

// Close ticker
func (t *Ticker) Close() {
	t.locker.Lock()
	defer t.locker.Unlock()
	t.closed = true
}

// NewTicker make a ticker every duration from hourglass
func (h *Hourglass) NewTicker(duration time.Duration) (*Ticker, error) {
	err := checkDuration(h.tickDuration, duration)
	if err != nil {
		return nil, err
	}
	ticker := Ticker{
		duration: duration,
		arg:      make(chan time.Time, 1),
		f:        sendTime,
		locker:   sync.RWMutex{},
	}
	h.rootWheel.setTicker(&ticker)
	return &ticker, nil
}

func (h *Hourglass) After(duration time.Duration) (*Ticker, error) {
	err := checkDuration(h.tickDuration, duration)
	if err != nil {
		return nil, err
	}
	ticker := Ticker{
		duration: duration,
		arg:      make(chan time.Time, 1),
		f:        sendTime,
		locker:   sync.RWMutex{},
	}
	defer ticker.Close()
	h.rootWheel.setTicker(&ticker)
	return &ticker, nil
}

func (h *Hourglass) AfterFunc(duration time.Duration, f func(t time.Time)) error {
	err := checkDuration(h.tickDuration, duration)
	if err != nil {
		return err
	}
	ticker := Ticker{
		duration: duration,
		arg:      f,
		f:        goFunc,
		locker:   sync.RWMutex{},
	}
	defer ticker.Close()
	h.rootWheel.setTicker(&ticker)
	return nil
}

func After(duration time.Duration) (*Ticker, error) {
	err := checkDuration(hourglass.tickDuration, duration)
	if err != nil {
		return nil, err
	}
	ticker := Ticker{
		duration: duration,
		arg:      make(chan time.Time, 1),
		f:        sendTime,
		locker:   sync.RWMutex{},
	}
	defer ticker.Close()
	hourglass.rootWheel.setTicker(&ticker)
	return &ticker, nil
}

func AfterFunc(duration time.Duration, f func(t time.Time)) error {
	err := checkDuration(hourglass.tickDuration, duration)
	if err != nil {
		return err
	}
	ticker := Ticker{
		duration: duration,
		arg:      f,
		f:        goFunc,
		locker:   sync.RWMutex{},
	}
	defer ticker.Close()
	hourglass.rootWheel.setTicker(&ticker)
	return nil
}

// NewTicker make a ticker use default hourglass with  1 second tickDuration and 60 ticksPerWheel
func NewTicker(duration time.Duration) (*Ticker, error) {
	return hourglass.NewTicker(duration)
}

// wheel is a ring queue and length is hourglass.ticksPerWheel
// currentTick is current tick in this wheel
// when currentTick less than hourglass.ticksPerWheel currentTick += 1
// when currentTick equal than hourglass.ticksPerWheel currentTick = 0 and wheel.next.tick()
// tickers is []Ticker for every currentTick
// all ticker in tickers[currentTick] will target in wheel.tick()
type wheel struct {
	currentTick   int
	tickDuration  time.Duration
	ticksPerWheel int
	tickers       []map[*Ticker]struct{}
	locker        sync.Mutex
	prev          *wheel
	next          *wheel
}

func (w *wheel) setTicker(ticker *Ticker) {
	w.locker.Lock()
	defer w.locker.Unlock()
	if ticker.closed {
		val, ok := ticker.arg.(chan time.Time)
		if ok {
			close(val)
		}
		return
	}
	ticks := int(ticker.duration / w.tickDuration)
	if ticks <= w.ticksPerWheel {
		//insert into current wheel
		tick := (ticks - 1 + w.currentTick) % w.ticksPerWheel
		if w.tickers[tick] == nil {
			w.tickers[tick] = make(map[*Ticker]struct{}, 8)
		}
		w.tickers[tick][ticker] = struct{}{}
		return
	}
	//next wheel
	if w.next == nil {
		w.next = &wheel{
			currentTick:   w.currentTick,
			tickDuration:  w.tickDuration * time.Duration(w.ticksPerWheel),
			ticksPerWheel: w.ticksPerWheel,
			tickers:       make([]map[*Ticker]struct{}, w.ticksPerWheel),
			locker:        sync.Mutex{},
			prev:          w,
		}
	}
	w.next.setTicker(ticker)
}

func (w *wheel) tick(t time.Time) {
	w.locker.Lock()
	defer w.locker.Unlock()
	for ticker := range w.tickers[w.currentTick] {
		go func(tk *Ticker) {
			tk.locker.RLock()
			defer tk.locker.RUnlock()
			tk.f(tk.arg, t)
			w.rootWheel().setTicker(tk)
		}(ticker)
		delete(w.tickers[w.currentTick], ticker)
	}
	w.currentTick += 1
	if w.currentTick == w.ticksPerWheel {
		w.currentTick = 0
		if w.next != nil {
			w.next.tick(t)
		}
	}
}

func (w *wheel) rootWheel() *wheel {
	if w.prev == nil {
		return w
	}
	return w.prev.rootWheel()
}

func checkDuration(tickDuration, duration time.Duration) error {
	if duration < tickDuration {
		return errors.New("duration must be grate than hourglass duration")
	}
	if duration%tickDuration != 0 {
		return errors.New("duration must be an integer multiple of the hourglass duration")
	}
	return nil
}

func sendTime(c any, t time.Time) {
	select {
	case c.(chan time.Time) <- t:
	default:
	}
}

func goFunc(f any, t time.Time) {
	go f.(func(time.Time))(t)
}
