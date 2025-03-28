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

package metanode

import (
	"fmt"
	"net"

	"github.com/cubefs/cubefs/datanode/storage"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	ForceClosedConnect = true
	NoClosedConnect    = false
)

func (m *metadataManager) IsForbiddenOp(mp MetaPartition, reqOp uint8) bool {
	if !mp.IsForbidden() {
		return false
	}
	switch reqOp {
	case
		// dentry
		proto.OpMetaCreateDentry,
		proto.OpMetaTxCreateDentry,
		proto.OpQuotaCreateDentry,
		proto.OpMetaDeleteDentry,
		proto.OpMetaTxDeleteDentry,
		proto.OpMetaBatchDeleteDentry,
		proto.OpMetaUpdateDentry,
		proto.OpMetaTxUpdateDentry,
		// extend
		proto.OpMetaUpdateXAttr,
		proto.OpMetaSetXAttr,
		proto.OpMetaBatchSetXAttr,
		proto.OpMetaRemoveXAttr,
		// extent
		proto.OpMetaTruncate,
		proto.OpMetaExtentsAdd,
		proto.OpMetaExtentAddWithCheck,
		proto.OpMetaObjExtentAdd,
		proto.OpMetaBatchObjExtentsAdd,
		proto.OpMetaBatchExtentsAdd,
		proto.OpMetaExtentsDel,
		// inode
		proto.OpMetaCreateInode,
		proto.OpQuotaCreateInode,
		proto.OpMetaTxUnlinkInode,
		proto.OpMetaUnlinkInode,
		proto.OpMetaBatchUnlinkInode,
		proto.OpMetaTxLinkInode,
		proto.OpMetaLinkInode,
		proto.OpMetaEvictInode,
		proto.OpMetaBatchEvictInode,
		proto.OpMetaSetattr,
		proto.OpMetaBatchDeleteInode,
		proto.OpMetaClearInodeCache,
		proto.OpMetaTxCreateInode,
		proto.OpMetaLookup,
		// multipart
		proto.OpAddMultipartPart,
		proto.OpRemoveMultipart,
		proto.OpCreateMultipart,
		// quota
		proto.OpMetaBatchSetInodeQuota,
		proto.OpMetaBatchDeleteInodeQuota:

		return true
	default:
		return false
	}
}

// The proxy is used during the leader change. When a leader of a partition changes, the proxy forwards the request to
// the new leader.
func (m *metadataManager) serveProxy(conn net.Conn, mp MetaPartition,
	p *Packet,
) (ok bool) {
	var (
		mConn      *net.TCPConn
		leaderAddr string
		err        error
		reqID      = p.ReqID
		reqOp      = p.Opcode
	)

	// check forbidden
	if m.IsForbiddenOp(mp, reqOp) {
		err = storage.ForbiddenMetaPartitionError
		p.PacketErrorWithBody(proto.OpForbidErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return false
	}

	followerRead := func() bool {
		if !p.IsReadMetaPkt() {
			return false
		}

		if p.ProtoVersion == proto.PacketProtoVersion0 && mp.IsFollowerRead() {
			return true
		}

		return p.IsFollowerReadMetaPkt()
	}

	if leaderAddr, ok = mp.IsLeader(); ok {
		return
	}

	if leaderAddr == "" {
		if followerRead() {
			log.LogDebugf("read from follower: p(%v), arg(%v)", p, mp.GetBaseConfig().PartitionId)
			return true
		}

		err = fmt.Errorf("mpId(%v) %v", mp.GetBaseConfig().PartitionId, ErrNoLeader)
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		goto end
	}

	mConn, err = m.connPool.GetConnect(leaderAddr)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.connPool.PutConnect(mConn, ForceClosedConnect)
		goto end
	}

	// send to master connection
	if err = p.WriteToConn(mConn); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.connPool.PutConnect(mConn, ForceClosedConnect)
		goto end
	}

	// read connection from the master
	if err = p.ReadFromConnWithVer(mConn, proto.NoReadDeadlineTime); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.connPool.PutConnect(mConn, ForceClosedConnect)
		goto end
	}
	if reqID != p.ReqID || reqOp != p.Opcode {
		log.LogErrorf("serveProxy: send and received packet mismatch: req(%v_%v) resp(%v_%v)",
			reqID, reqOp, p.ReqID, p.Opcode)
	}
	m.connPool.PutConnect(mConn, NoClosedConnect)

end:
	if p.Opcode != proto.OpOk && followerRead() {
		log.LogWarnf("read from follower after try leader failed: p(%v)", p)
		return true
	}

	m.respondToClient(conn, p)
	if err != nil {
		log.LogErrorf("[serveProxy]: req: %d - %v, %v, packet(%v)", p.GetReqID(),
			p.GetOpMsg(), err, p)
	}
	log.LogDebugf("[serveProxy] req: %d - %v, resp: %v, packet(%v)", p.GetReqID(), p.GetOpMsg(),
		p.GetResultMsg(), p)
	return
}
