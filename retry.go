// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

// This file is based on code (c) 2014 Cenk Altı and governed by the MIT license.
// See https://github.com/cenkalti/backoff for original source.

package opensearch

import "time"

// An Operation is executing by Retry() or RetryNotify().
// The operation will be retried using a backoff policy if it returns an error.
type Operation func() error

// Notify is a notify-on-error function. It receives error returned
// from an operation.
//
// Notice that if the backoff policy stated to stop retrying,
// the notify function isn't called.
type Notify func(error)

// Retry the function f until it does not return error or BackOff stops.
// f is guaranteed to be run at least once.
// It is the caller's responsibility to reset b after Retry returns.
//
// Retry sleeps the goroutine for the duration returned by BackOff after a
// failed operation returns.
func Retry(o Operation, b Backoff) error { return RetryNotify(o, b, nil) }

// RetryNotify calls notify function with the error and wait duration
// for each failed attempt before sleep.
func RetryNotify(operation Operation, b Backoff, notify Notify) error {
	var err error
	var wait time.Duration
	var retry bool
	var n int

	for {
		if err = operation(); err == nil {
			return nil
		}

		n++
		wait, retry = b.Next(n)
		if !retry {
			return err
		}

		if notify != nil {
			notify(err)
		}

		time.Sleep(wait)
	}
}
