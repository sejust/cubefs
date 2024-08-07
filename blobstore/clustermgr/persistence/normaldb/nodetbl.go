// Copyright 2024 The CubeFS Authors.
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

package normaldb

import (
	"encoding/json"

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	NodeInfoVersionNormal = iota + 1
)

type NodeInfoRecord struct {
	Version   uint8            `json:"-"`
	NodeID    proto.NodeID     `json:"node_id"`
	NodeSetID proto.NodeSetID  `json:"node_set_id"`
	ClusterID proto.ClusterID  `json:"cluster_id"`
	DiskType  proto.DiskType   `json:"disk_type"`
	Idc       string           `json:"idc"`
	Rack      string           `json:"rack"`
	Host      string           `json:"host"`
	Role      proto.NodeRole   `json:"role"`
	Status    proto.NodeStatus `json:"status"`
}

type NodeTable struct {
	tbl kvstore.KVTable
}

func OpenNodeTable(db kvstore.KVStore) (*NodeTable, error) {
	if db == nil {
		return nil, errors.New("OpenNodeTable failed: db is nil")
	}
	table := &NodeTable{
		tbl: db.Table(nodeCF),
	}
	return table, nil
}

func (s *NodeTable) GetAllNodes() ([]*NodeInfoRecord, error) {
	iter := s.tbl.NewIterator(nil)
	defer iter.Close()

	ret := make([]*NodeInfoRecord, 0)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return nil, iter.Err()
		}
		if iter.Key().Size() > 0 {
			info, err := decodeNodeInfoRecord(iter.Value().Data())
			if err != nil {
				return nil, errors.Info(err, "decode node info db failed").Detail(err)
			}
			ret = append(ret, info)
			iter.Key().Free()
			iter.Value().Free()
		}
	}
	return ret, nil
}

func (n *NodeTable) UpdateNode(info *NodeInfoRecord) error {
	key := info.NodeID.Encode()
	value, err := encodeNodeInfoRecord(info)
	if err != nil {
		return err
	}

	err = n.tbl.Put(kvstore.KV{Key: key, Value: value})
	if err != nil {
		return err
	}
	return nil
}

func encodeNodeInfoRecord(info *NodeInfoRecord) ([]byte, error) {
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func decodeNodeInfoRecord(data []byte) (*NodeInfoRecord, error) {
	version := data[0]
	if version == NodeInfoVersionNormal {
		ret := &NodeInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid node info version")
}
