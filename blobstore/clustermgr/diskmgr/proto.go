package diskmgr

import (
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type nodeItem struct {
	nodeID proto.NodeID
	info   *blobnode.NodeInfo
	disks  map[proto.DiskID]*blobnode.DiskInfo

	lock sync.RWMutex
}

func (n *nodeItem) isUsingStatus() bool {
	return n.info.Status != proto.NodeStatusDropped
}

func (n *nodeItem) genFilterKey() string {
	return n.info.Host + n.info.DiskType.String()
}

type diskItem struct {
	diskID         proto.DiskID
	info           *blobnode.DiskInfo
	expireTime     time.Time
	lastExpireTime time.Time
	dropping       bool

	lock sync.RWMutex
}

func (d *diskItem) isExpire() bool {
	if d.expireTime.IsZero() {
		return false
	}
	return time.Since(d.expireTime) > 0
}

func (d *diskItem) isAvailable() bool {
	if d.info.Readonly || d.info.Status != proto.DiskStatusNormal || d.dropping {
		return false
	}
	return true
}

// isWritable return false if disk heartbeat expire or disk status is not normal or disk is readonly or dropping
func (d *diskItem) isWritable() bool {
	if d.isExpire() || !d.isAvailable() {
		return false
	}
	return true
}

func (d *diskItem) needFilter() bool {
	return d.info.Status != proto.DiskStatusRepaired && d.info.Status != proto.DiskStatusDropped
}

func (d *diskItem) genFilterKey() string {
	return d.info.Host + d.info.Path
}

func (d *diskItem) withRLocked(f func() error) error {
	d.lock.RLock()
	defer d.lock.RUnlock()

	return f()
}

func (d *diskItem) withLocked(f func() error) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	return f()
}
