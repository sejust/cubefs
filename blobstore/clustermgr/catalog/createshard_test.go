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
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/cluster"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	mock "github.com/cubefs/cubefs/blobstore/testing/mockclustermgr"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func TestCatalogMgr_CreateShard(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockRaftServer.EXPECT().Status().AnyTimes().Return(raftserver.Status{Id: 1})
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockDiskMgr := cluster.NewMockShardNodeManagerAPI(ctr)
	mockDiskMgr.EXPECT().AllocShards(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, policy cluster.AllocShardsPolicy) ([]proto.DiskID, []proto.Suid, error) {
		diskIDs := make([]proto.DiskID, len(policy.Suids))
		for i := range diskIDs {
			diskIDs[i] = proto.DiskID(i + 1)
		}
		return diskIDs, policy.Suids, nil
	})
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockGetDiskInfo)
	mockCatalogMgr.raftServer = mockRaftServer
	mockCatalogMgr.scopeMgr = mockScopeMgr
	mockCatalogMgr.diskMgr = mockDiskMgr

	// success case
	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(31), uint64(158), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		err := mockCatalogMgr.createShard(ctx)
		require.NoError(t, err)
	}

	// az unavailable, create shard
	{
		testConfig.UnavailableIDC = "z0"
		oldCodeMode := testConfig.CodeMode
		defer func() {
			testConfig.UnavailableIDC = ""
			testConfig.CodeMode = oldCodeMode
		}()
		testConfig.CodeMode = codemode.Replica4TwoAZ
		catalogMgr, clean := initMockCatalogMgr(t, testConfig)
		defer clean()

		catalogMgr.raftServer = mockRaftServer
		catalogMgr.scopeMgr = mockScopeMgr
		catalogMgr.diskMgr = mockDiskMgr

		// create 2AZ success
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(51), uint64(51), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		err := catalogMgr.createShard(ctx)
		require.NoError(t, err)

		// one az Unavailable ,create 3AZ failed
		catalogMgr.CodeMode = codemode.Replica3
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(52), uint64(52), nil)
		err = catalogMgr.createShard(ctx)
		require.Error(t, err)

	}
}

func TestCatalogMgr_finishLastCreateJob(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockCatalogMgr.raftServer = mockRaftServer
	mockDiskMgr := cluster.NewMockShardNodeManagerAPI(ctr)
	mockRaftServer.EXPECT().Status().AnyTimes().Return(raftserver.Status{Id: 1})
	allocSuccess := func() {
		mockDiskMgr.EXPECT().AllocShards(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, policy cluster.AllocShardsPolicy) ([]proto.DiskID, []proto.Suid, error) {
			diskids := make([]proto.DiskID, len(policy.Suids))
			for i := range diskids {
				diskids[i] = proto.DiskID(i + 1)
			}
			return diskids, policy.Suids, nil
		})
	}
	allocFailed := func(n int) {
		mockDiskMgr.EXPECT().AllocShards(gomock.Any(), gomock.Any()).MaxTimes(n).Return(nil, proto.DiskSetID(0), cluster.ErrNoEnoughSpace)
	}
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockGetDiskInfo)
	mockCatalogMgr.scopeMgr = mockScopeMgr
	mockCatalogMgr.diskMgr = mockDiskMgr

	{
		mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(31), uint64(31), nil)
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, data []byte) interface{} {
			proposeInfo := base.DecodeProposeInfo(data)
			if proposeInfo.OperType == OperTypeInitCreateShard {
				args := &initCreateShardCtx{}
				err := json.Unmarshal(proposeInfo.Data, args)
				require.NoError(t, err)
				err = mockCatalogMgr.applyInitCreateShard(ctx, args)
				require.NoError(t, err)
			}
			return nil
		})
		allocFailed(3)
		err := mockCatalogMgr.createShard(ctx)
		require.Error(t, err)
		allocSuccess()
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		err = mockCatalogMgr.finishLastCreateJob(ctx)
		require.NoError(t, err)
	}
}

func TestCatalogMgr_applyCreateShard(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	existingShard := mockCatalogMgr.allShards.getShard(1)
	require.NotNil(t, existingShard)

	newShardID := proto.ShardID(99)
	unitCount := len(existingShard.unitEpochs)
	unitEpochs := make([]*shardUnitEpoch, unitCount)
	units := make([]clustermgr.ShardUnit, unitCount)
	for i, ue := range existingShard.unitEpochs {
		suid := proto.EncodeSuid(newShardID, ue.suidPrefix.Index(), proto.MinEpoch)
		unitEpochs[i] = &shardUnitEpoch{
			suidPrefix: suid.SuidPrefix(),
			epoch:      suid.Epoch(),
			nextEpoch:  suid.Epoch(),
		}
		units[i] = clustermgr.ShardUnit{
			Suid:   suid,
			DiskID: proto.DiskID(i + 1),
		}
	}
	newShard := &shardItem{
		shardID:    newShardID,
		unitEpochs: unitEpochs,
		info: shardInfoBase{
			Shard: clustermgr.Shard{
				ShardID: newShardID,
				Range:   existingShard.info.Range,
				Units:   units,
			},
		},
	}

	expectedRouteVersion := proto.RouteVersion(mockCatalogMgr.routeMgr.GetRouteVersion() + 1)
	err := mockCatalogMgr.applyCreateShard(ctx, newShard)
	require.NoError(t, err)
	got := mockCatalogMgr.allShards.getShard(newShardID)
	require.NotNil(t, got)
	require.Equal(t, expectedRouteVersion, got.info.RouteVersion)

	shardRecord, err := mockCatalogMgr.catalogTbl.GetShard(newShardID)
	require.NoError(t, err)
	require.Equal(t, expectedRouteVersion, shardRecord.RouteVersion)
}
