// Go MySQL Driver - A MySQL-Driver for Go's database/sql package.
//
// Copyright 2022 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
//go:build !go1.19
// +build !go1.19

package mysql

import "sync/atomic"

/******************************************************************************
*                               Sync utils                                    *
******************************************************************************/

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://github.com/golang/go/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}

// Unlock is a no-op used by -copylocks checker from `go vet`.
// noCopy should implement sync.Locker from Go 1.11
// https://github.com/golang/go/commit/c2eba53e7f80df21d51285879d51ab81bcfbf6bc
// https://github.com/golang/go/issues/26165
func (*noCopy) Unlock() {}

// atomicBool is an implementation of atomic.Bool for older version of Go.
// it is a wrapper around uint32 for usage as a boolean value with
// atomic access.
type atomicBool struct {
	_     noCopy
	value uint32
}

// Load returns whether the current boolean value is true
func (ab *atomicBool) Load() bool {
	return atomic.LoadUint32(&ab.value) > 0
}

// Store sets the value of the bool regardless of the previous value
func (ab *atomicBool) Store(value bool) {
	if value {
		atomic.StoreUint32(&ab.value, 1)
	} else {
		atomic.StoreUint32(&ab.value, 0)
	}
}

// Swap sets the value of the bool and returns the old value.
func (ab *atomicBool) Swap(value bool) bool {
	if value {
		return atomic.SwapUint32(&ab.value, 1) > 0
	}
	return atomic.SwapUint32(&ab.value, 0) > 0
}
