// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import "sync/atomic"

type LimitCounter struct {
	cntLimit     *uint64
	defaultValue uint64
}

func newLimitCounter(cntLimit *uint64, defaultValue uint64) LimitCounter {
	limiter := LimitCounter{
		cntLimit:     cntLimit,
		defaultValue: defaultValue,
	}
	return limiter
}

func (cntLimiter *LimitCounter) GetCntLimit() uint64 {
	limit := uint64(0)
	if cntLimiter.cntLimit != nil {
		limit = atomic.LoadUint64(cntLimiter.cntLimit)
	}
	if limit == 0 {
		limit = cntLimiter.defaultValue
	}
	return limit
}
