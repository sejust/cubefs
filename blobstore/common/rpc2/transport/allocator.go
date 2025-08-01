package transport

import (
	"sync"
)

// AssignedBuffer assigned buffer from Allocator.
type AssignedBuffer interface {
	// Bytes returns plain bytes.
	Bytes() []byte
	// Written sets efficient n of bytes.
	Written(n int)
	// Len returns efficient length of bytes.
	Len() int
	// Free back buffer to allocator.
	Free() error
}

// Allocator reused buffer for frames.
type Allocator interface {
	// Alloc returns size length of assigned buffer.
	Alloc(size int) (AssignedBuffer, error)
}

var (
	defaultAllocator Allocator
	debruijinPos     = [...]byte{
		0, 9, 1, 10, 13, 21, 2, 29, 11, 14,
		16, 18, 22, 25, 3, 30, 8, 12, 20, 28,
		15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31,
	}
)

const maxsize = 1 << 24

func init() {
	defaultAllocator = NewAllocator()
}

// allocator for incoming frames, optimized to prevent overwriting after zeroing
type allocator struct {
	buffers []sync.Pool
}

// NewAllocator initiates a []byte allocator for frames,
// the waste(memory fragmentation) of space allocation is guaranteed to be
// no more than 50%.
func NewAllocator() Allocator {
	alloc := new(allocator)
	alloc.buffers = make([]sync.Pool, 25) // 1B -> 16MB
	for k := range alloc.buffers {
		i := k
		alloc.buffers[k].New = func() interface{} {
			return make([]byte, 1<<uint32(i))
		}
	}
	return alloc
}

// Alloc a bytes from pool with most appropriate cap
func (alloc *allocator) Alloc(size int) (AssignedBuffer, error) {
	if size <= 0 || size > maxsize {
		return nil, ErrAllocOversize
	}

	var buffer []byte
	bits := msb(size)
	if size == 1<<bits {
		buffer = alloc.buffers[bits].Get().([]byte)[:size]
	} else {
		buffer = alloc.buffers[bits+1].Get().([]byte)[:size]
	}
	return &assignedBuffer{alloc: alloc, buffer: buffer}, nil
}

// Free returns a bytes to pool for future use,
// which the cap must be exactly 2^n
func (alloc *allocator) Free(buf []byte) error {
	bits := msb(cap(buf))
	if cap(buf) == 0 || cap(buf) > maxsize || cap(buf) != 1<<bits {
		return ErrAllocOversize
	}
	alloc.buffers[bits].Put(buf) // nolint: staticcheck
	return nil
}

type assignedBuffer struct {
	offset int
	buffer []byte
	alloc  *allocator
}

func (ab *assignedBuffer) Bytes() []byte { return ab.buffer }
func (ab *assignedBuffer) Written(n int) { ab.offset += n }
func (ab *assignedBuffer) Len() int      { return ab.offset }
func (ab *assignedBuffer) Free() (err error) {
	if ab.alloc == nil {
		return
	}
	err = ab.alloc.Free(ab.buffer)
	ab.alloc = nil
	ab.buffer = nil
	return
}

// msb return the pos of most significiant bit
// http://supertech.csail.mit.edu/papers/debruijn.pdf
func msb(size int) byte {
	v := uint32(size)
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	return debruijinPos[(v*0x07C4ACDD)>>27]
}
