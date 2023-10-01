// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"net"
	"time"
)

const defaultBufSize = 4 * 1024

// const maxCachedBufSize = 256 * 1024

// A buffer which is used for both reading and writing.
// This is possible since communication on each connection is synchronous.
// In other words, we can't write and read simultaneously on the same connection.
// The buffer is similar to bufio.Reader / Writer but zero-copy-ish
// Also highly optimized for this particular use case.
// This buffer is backed by two byte slices in a double-buffering scheme
type buffer struct {
	buf     []byte // buf is a byte buffer who's length and capacity are equal.
	nc      net.Conn
	length  int
	timeout time.Duration
	dbuf    [2][]byte // dbuf is an array with the two byte slices that back this buffer
	flipcnt uint      // flipccnt is the current buffer counter for double-buffering
}

// newBuffer allocates and returns a new buffer.
func newBuffer(nc net.Conn) buffer {
	fg := make([]byte, defaultBufSize)
	return buffer{
		buf:  fg,
		nc:   nc,
		dbuf: [2][]byte{fg, nil},
	}
}

// flip replaces the active buffer with the background buffer
// this is a delayed flip that simply increases the buffer counter;
// the actual flip will be performed the next time we call `buffer.fill`
func (b *buffer) flip() {
	b.flipcnt += 1
}

// takeBuffer returns a buffer with the requested size.
// If possible, a slice from the existing buffer is returned.
// Otherwise a bigger buffer is made.
// Only one buffer (total) can be used at a time.
func (b *buffer) takeBuffer(length int) ([]byte, error) {
	if b.length > 0 {
		return nil, ErrBusyBuffer
	}

	// test (cheap) general case first
	if length <= cap(b.buf) {
		return b.buf[:length], nil
	}

	if length < maxPacketSize {
		b.buf = make([]byte, length)
		return b.buf, nil
	}

	// buffer is larger than we want to store.
	return make([]byte, length), nil
}

// takeSmallBuffer is shortcut which can be used if length is
// known to be smaller than defaultBufSize.
// Only one buffer (total) can be used at a time.
func (b *buffer) takeSmallBuffer(length int) ([]byte, error) {
	if b.length > 0 {
		return nil, ErrBusyBuffer
	}
	return b.buf[:length], nil
}

// takeCompleteBuffer returns the complete existing buffer.
// This can be used if the necessary buffer size is unknown.
// cap and len of the returned buffer will be equal.
// Only one buffer (total) can be used at a time.
func (b *buffer) takeCompleteBuffer() ([]byte, error) {
	if b.length > 0 {
		return nil, ErrBusyBuffer
	}
	return b.buf, nil
}

// store stores buf, an updated buffer, if its suitable to do so.
func (b *buffer) store(buf []byte) error {
	if b.length > 0 {
		return ErrBusyBuffer
	} else if cap(buf) <= maxPacketSize && cap(buf) > cap(b.buf) {
		b.buf = buf[:cap(buf)]
	}
	return nil
}
