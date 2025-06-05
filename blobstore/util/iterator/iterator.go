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

package iterator

import "context"

type (
	Iterate[I, O any] func(context.Context, *I, *O) error

	Iterator[I, O any] interface {
		Next(context.Context) (error, bool)
		Input() *I
		Output() *O
		Close()
	}

	IterIn interface {
		Input() any // any must be *I
		Update(any) // any must be *O
		Close()
	}
	In[I, O any] struct {
		IterIn
	}

	IterOut interface {
		Done() bool
		Output() any // any must be *O
		Merge(any)   // any must be *O
		Close()
	}
	Out[O any] struct {
		IterOut
	}
)

func (o Out[O]) Done() bool     { return o.IterOut.Done() }
func (o Out[O]) Output() *O     { return o.IterOut.Output().(*O) }
func (o Out[O]) Merge(other *O) { o.IterOut.Merge(other) }

func (i In[I, O]) Input() *I   { return i.IterIn.Input().(*I) }
func (i In[I, O]) Update(o *O) { i.IterIn.Update(o) }

func Merged[I, O any](ctx context.Context,
	iterate Iterate[I, O], in In[I, O], out Out[O],
) (err error) {
	_out := new(O)
	for {
		if err = iterate(ctx, in.Input(), _out); err != nil {
			return
		}
		in.Update(_out)
		out.Merge(_out)
		if out.Done() {
			return
		}
	}
}

type iterator[I, O any] struct {
	in   In[I, O]
	out  Out[O]
	next Iterate[I, O]
	err  error
}

func New[I, O any](iterate Iterate[I, O], in In[I, O], out Out[O]) Iterator[I, O] {
	return &iterator[I, O]{in: in, out: out, next: iterate}
}

func (i *iterator[I, O]) Next(ctx context.Context) (error, bool) {
	if i.err != nil {
		return i.err, true
	}
	if i.out.Done() {
		return nil, false
	}
	if i.err = i.next(ctx, i.in.Input(), i.out.Output()); i.err != nil {
		return i.err, true
	}
	i.in.Update(i.out.Output())
	return nil, true
}
func (i *iterator[I, O]) Input() *I  { return i.in.Input() }
func (i *iterator[I, O]) Output() *O { return i.out.Output() }
func (i *iterator[I, O]) Close()     { i.in.Close(); i.out.Close() }
