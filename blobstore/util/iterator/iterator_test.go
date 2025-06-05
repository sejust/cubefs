// Copyright 2025 The CubeFS Authors.
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

package iterator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/iterator"
	"github.com/stretchr/testify/require"
)

type in struct {
	marker int
}

func (i *in) Input() any    { return i }
func (i *in) Update(oa any) { o := oa.(*out); i.marker = o.next }
func (i *in) Close()        { i.marker = 0 }

type out struct {
	ints []int
	next int
}

func (o *out) Done() bool  { return o.next < 0 }
func (o *out) Output() any { return o }
func (o *out) Close()      { o.next = 0 }
func (o *out) Merge(other any) {
	oo := other.(*out)
	o.ints = append(o.ints, oo.ints...)
	o.next = oo.next
}

var (
	_ iterator.IterIn  = (*in)(nil)
	_ iterator.IterOut = (*out)(nil)
)

var next iterator.Iterate[in, out] = func(_ context.Context, i *in, o *out) error {
	o.ints = make([]int, 0, 10)
	next := i.marker
	for next < 101 && len(o.ints) < 10 {
		next++
		o.ints = append(o.ints, next)
	}
	if next >= 101 {
		o.next = -1
	} else {
		o.next = next
	}
	return nil
}

var nexterr iterator.Iterate[in, out] = func(context.Context, *in, *out) error {
	return errors.New("next error")
}

func TestUtilIteratorMerged(t *testing.T) {
	i, o := new(in), new(out)
	require.NoError(t, iterator.Merged(context.Background(),
		next, iterator.In[in, out]{i}, iterator.Out[out]{o}))
	require.Equal(t, 101, len(o.ints))
	require.Error(t, iterator.Merged(context.Background(),
		nexterr, iterator.In[in, out]{i}, iterator.Out[out]{o}))
}

func TestUtilIteratorNext(t *testing.T) {
	i, o := new(in), new(out)
	iter := iterator.New(next, iterator.In[in, out]{i}, iterator.Out[out]{o})
	all := 0
	for {
		err, hasnext := iter.Next(context.Background())
		if !hasnext {
			break
		}
		require.NoError(t, err)
		o := iter.Output()
		all += len(o.ints)
		require.Equal(t, iter.Input().marker, o.next)
	}
	require.Equal(t, 101, all)
	iter.Close()
	require.Equal(t, 0, i.marker)
	require.Equal(t, 0, o.next)

	i, o = new(in), new(out)
	iter = iterator.New(nexterr, iterator.In[in, out]{i}, iterator.Out[out]{o})
	err, hasnext := iter.Next(context.Background())
	require.True(t, hasnext)
	require.Error(t, err)
	err, hasnext = iter.Next(context.Background())
	require.True(t, hasnext)
	require.Error(t, err)
	iter.Close()
}
