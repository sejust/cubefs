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

package catalog

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/cluster"
	"github.com/cubefs/cubefs/blobstore/clustermgr/kvmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// Config defines catalog manager configuration
type Config struct {
	CatalogDBPath                string            `json:"catalog_db_path"`
	FlushIntervalS               int               `json:"flush_interval_s"`
	ShardConcurrentMapNum        uint32            `json:"shard_concurrent_map_num"`
	SpaceConcurrentMapNum        uint32            `json:"space_concurrent_map_num"`
	ApplyConcurrency             uint32            `json:"apply_concurrency"`
	InitShardNum                 int               `json:"init_shard_num"`
	CheckInitShardIntervalS      int               `json:"check_init_shard_interval_s"`
	RouteItemTruncateIntervalNum uint32            `json:"route_item_truncate_interval_num"`
	ShardNodeConfig              shardnode.Config  `json:"shard_node_config"`
	CodeMode                     codemode.CodeMode `json:"-"`

	IDC            []string        `json:"-"`
	UnavailableIDC string          `json:"-"`
	Region         string          `json:"-"`
	ClusterID      proto.ClusterID `json:"-"`
}

func (cfg *Config) checkAndFix() {
	defaulter.LessOrEqual(&cfg.FlushIntervalS, defaultFlushIntervalS)
	defaulter.LessOrEqual(&cfg.ShardConcurrentMapNum, defaultShardConcurrentMapNum)
	defaulter.LessOrEqual(&cfg.SpaceConcurrentMapNum, defaultSpaceConcurrentMapNum)
	defaulter.LessOrEqual(&cfg.ApplyConcurrency, defaultApplyConcurrency)
	defaulter.LessOrEqual(&cfg.CheckInitShardIntervalS, defaultCheckInitShardIntervalS)
	defaulter.LessOrEqual(&cfg.RouteItemTruncateIntervalNum, defaultRouteItemTruncateIntervalNum)
}

func NewCatalogMgr(conf Config, diskMgr cluster.ShardNodeManagerAPI, scopeMgr scopemgr.ScopeMgrAPI, kvMgr kvmgr.KvMgrAPI, catalogDB kvstore.KVStore) (
	*CatalogMgr, error,
) {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "new-catalog-mgr")
	conf.checkAndFix()
	catalogTable, err := catalogdb.OpenCatalogTable(catalogDB)
	if err != nil {
		return nil, errors.Info(err, "open catalog table failed").Detail(err)
	}
	transitedTable, err := catalogdb.OpenTransitedTable(catalogDB)
	if err != nil {
		return nil, errors.Info(err, "open transited table failed").Detail(err)
	}

	// initial catalogMgr
	catalogMgr := &CatalogMgr{
		allShards:    newConcurrentShards(conf.ShardConcurrentMapNum),
		allSpaces:    newConcurrentSpaces(conf.SpaceConcurrentMapNum),
		catalogTbl:   catalogTable,
		transitedTbl: transitedTable,

		applyTaskPool:   base.NewTaskDistribution(int(conf.ApplyConcurrency), 1),
		scopeMgr:        scopeMgr,
		kvMgr:           kvMgr,
		routeMgr:        newRouteMgr(conf.RouteItemTruncateIntervalNum, catalogTable),
		diskMgr:         diskMgr,
		shardNodeClient: shardnode.New(conf.ShardNodeConfig),
		closeLoopChan:   make(chan struct{}, 1),
		Config:          conf,
	}

	// initial dirty shards
	catalogMgr.dirty.Store(newConcurrentShards(conf.ShardConcurrentMapNum))

	// initial load data
	if err := catalogMgr.LoadData(ctx); err != nil {
		return nil, err
	}

	return catalogMgr, nil
}

func (c *CatalogMgr) SetRaftServer(raftServer raftserver.RaftServer) {
	c.raftServer = raftServer
}

func (c *CatalogMgr) Start() {
	go c.loop()
	go c.routeLoop()
}

func (c *CatalogMgr) Close() {
	close(c.closeLoopChan)
	c.routeMgr.Close()
}

func (c *CatalogMgr) loadShard(ctx context.Context) error {
	return c.catalogTbl.RangeShardRecord(func(shardRecord *catalogdb.ShardInfoRecord) error {
		shardUnitEpochs := make([]*shardUnitEpoch, 0, len(shardRecord.SuidPrefixes))
		shardInfoUnits := make([]clustermgr.ShardUnit, 0, len(shardRecord.SuidPrefixes))

		for _, suidPrefix := range shardRecord.SuidPrefixes {
			unitRecord, err := c.catalogTbl.GetShardUnit(suidPrefix)
			if err != nil {
				return errors.Info(err, "get shard unit error")
			}
			diskInfo, err := c.diskMgr.GetDiskInfo(ctx, unitRecord.DiskID)
			if err != nil {
				return errors.Info(err, "get disk info error,diskID:", unitRecord.DiskID)
			}
			unit, epoch := shardUnitRecordToShardUnit(unitRecord)
			unit.Host = diskInfo.Host
			shardUnitEpochs = append(shardUnitEpochs, epoch)
			shardInfoUnits = append(shardInfoUnits, unit)
		}

		shardInfo := shardRecordToShardInfo(shardRecord, shardInfoUnits)
		shard := &shardItem{
			shardID:    shardInfo.ShardID,
			unitEpochs: shardUnitEpochs,
			info:       shardInfo,
		}
		c.allShards.putShard(shard)
		return nil
	})
}

func (c *CatalogMgr) loadSpace(ctx context.Context) error {
	return c.catalogTbl.RangeSpaceRecord(func(spaceRecord *catalogdb.SpaceInfoRecord) error {
		spaceInfo := spaceRecordToSpaceInfo(spaceRecord)
		space := &spaceItem{
			spaceID: spaceInfo.SpaceID,
			info:    spaceInfo,
		}

		c.allSpaces.putSpace(space)
		return nil
	})
}

func (c *CatalogMgr) loadRoute(ctx context.Context) error {
	return c.routeMgr.loadRoute(ctx)
}
