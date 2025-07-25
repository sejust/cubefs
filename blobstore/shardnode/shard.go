// Copyright 2022 The CubeFS Authors.
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

// nolint
package shardnode

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (s *service) addShard(ctx context.Context, req *shardnode.AddShardArgs) error {
	disk, err := s.getDisk(req.DiskID)
	if err != nil {
		return err
	}

	return disk.AddShard(ctx, req.Suid, req.RouteVersion, req.Range, req.Units)
}

// UpdateShard update shard info
func (s *service) updateShard(ctx context.Context, req *shardnode.UpdateShardArgs) error {
	disk, err := s.getDisk(req.DiskID)
	if err != nil {
		return err
	}

	return disk.UpdateShard(ctx, req.Suid, req.ShardUpdateType, req.Unit)
}

// transferShardLeader transfer shard leader
func (s *service) transferShardLeader(ctx context.Context, req *shardnode.TransferShardLeaderArgs) error {
	shard, err := s.GetShard(req.DiskID, req.Suid)
	if err != nil {
		return err
	}

	return shard.TransferLeader(ctx, req.GetDestDiskID())
}

func (s *service) getShardUintInfo(ctx context.Context, diskID proto.DiskID, suid proto.Suid) (ret clustermgr.ShardUnitInfo, err error) {
	shard, err := s.GetShard(diskID, suid)
	if err != nil {
		return
	}

	shardStat, err := shard.Stats(ctx, true)
	if err != nil {
		return
	}

	return clustermgr.ShardUnitInfo{
		Suid:         suid,
		DiskID:       diskID,
		AppliedIndex: shardStat.AppliedIndex,
		LeaderDiskID: shardStat.LeaderDiskID,
		Range:        shardStat.Range,
		RouteVersion: shardStat.RouteVersion,
	}, nil
}

func (s *service) getShardStats(ctx context.Context, diskID proto.DiskID, suid proto.Suid) (ret shardnode.ShardStats, err error) {
	shard, err := s.GetShard(diskID, suid)
	if err != nil {
		return
	}

	shardStat, err := shard.Stats(ctx, true)
	if err != nil {
		return
	}

	return shardStat, nil
}

func (s *service) listVolume(cxt context.Context, mode codemode.CodeMode) ([]clustermgr.AllocVolumeInfo, error) {
	return s.catalog.ListVolume(cxt, mode)
}

func (s *service) listShards(ctx context.Context, diskID proto.DiskID, shardID proto.ShardID, count uint64) (ret []shardnode.ListShardBaseInfo, err error) {
	disk, err := s.getDisk(diskID)
	if err != nil {
		return
	}

	ret = make([]shardnode.ListShardBaseInfo, 0, count)
	shards := make([]storage.ShardHandler, 0, count)
	disk.RangeShardNoRWCheck(func(s storage.ShardHandler) bool {
		if shardID != proto.InvalidShardID {
			if s.GetSuid().ShardID() != shardID {
				return true
			}
			shards = append(shards, s)
			return false
		}
		shards = append(shards, s)
		return true
	})
	for _, shard := range shards {
		if count == 0 {
			return
		}
		suid := shard.GetSuid()
		ret = append(ret, shardnode.ListShardBaseInfo{
			Suid:    suid,
			ShardID: suid.ShardID(),
			DiskID:  diskID,
			Index:   uint32(suid.Index()),
			Epoch:   suid.Epoch(),
			Units:   shard.GetUnits(),
		})
		if suid.ShardID() == shardID {
			break
		}
		count--
	}

	return
}

func (s *service) dbStats(ctx context.Context, req *shardnode.DBStatsArgs) (ret shardnode.DBStatsRet, err error) {
	span := trace.SpanFromContextSafe(ctx)
	disk, err := s.getDisk(req.DiskID)
	if err != nil {
		return
	}
	stats, err := disk.DBStats(ctx, req.DBName)
	if err != nil {
		span.Errorf("get db stats failed, err: %s", err.Error())
		return
	}
	ret = shardnode.DBStatsRet{
		Used:                stats.Used,
		BlobCacheUsage:      stats.MemoryUsage.BlockCacheUsage,
		IndexAndFilterUsage: stats.MemoryUsage.IndexAndFilterUsage,
		MemtableUsage:       stats.MemoryUsage.MemtableUsage,
		BlockPinnedUsage:    stats.MemoryUsage.BlockPinnedUsage,
		TotalMemoryUsage:    stats.MemoryUsage.Total,
	}
	return
}

func (s *service) GetShard(diskID proto.DiskID, suid proto.Suid) (storage.ShardHandler, error) {
	disk, err := s.getDisk(diskID)
	if err != nil {
		err = errors.Info(err, "get disk failed")
		return nil, err
	}
	sh, err := disk.GetShard(suid)
	if err != nil {
		err = errors.Info(err, "disk get shard failed")
		return nil, err
	}
	return sh, nil
}

func (s *service) loop(ctx context.Context) {
	heartbeatTicker := time.NewTicker(time.Duration(s.cfg.HeartBeatIntervalS) * time.Second)
	reportTicker := time.NewTicker(time.Duration(s.cfg.ReportIntervalS) * time.Second)
	routeUpdateTicker := time.NewTicker(time.Duration(s.cfg.RouteUpdateIntervalS) * time.Second)
	checkpointTicker := time.NewTicker(time.Duration(s.cfg.CheckPointIntervalM) * time.Minute)
	trashShardCheckTicker := time.NewTicker(time.Duration(s.cfg.ShardCheckAndClearIntervalH) * time.Hour)

	defer func() {
		heartbeatTicker.Stop()
		reportTicker.Stop()
		routeUpdateTicker.Stop()
		checkpointTicker.Stop()
	}()

	var span trace.Span
	diskReports := make([]clustermgr.ShardNodeDiskHeartbeatInfo, 0)
	shardReports := make([]clustermgr.ShardUnitInfo, 0, 1<<10)
	shards := make([]storage.ShardHandler, 0, 1<<10)
	tasks := make([]clustermgr.ShardTask, 0, 1<<10)

	for {
		select {
		case <-heartbeatTicker.C:
			span, ctx = trace.StartSpanFromContext(ctx, "")
			diskReports = diskReports[:0]

			disks := s.getAllDisks()
			for _, disk := range disks {
				diskInfo := disk.GetDiskInfo()
				diskReports = append(diskReports, clustermgr.ShardNodeDiskHeartbeatInfo{
					DiskID:       diskInfo.DiskID,
					Used:         diskInfo.Used,
					Size:         diskInfo.Size,
					Free:         diskInfo.Free,
					UsedShardCnt: int32(disk.GetShardCnt()),
				})
			}
			if err := s.transport.HeartbeatDisks(ctx, diskReports); err != nil {
				span.Warnf("heartbeat to master failed: %s", err)
			}

		case <-reportTicker.C:
			shardReports = shardReports[:0]
			s.shardReports(ctx, shards, shardReports, false)
			reportTicker.Reset(time.Duration(s.cfg.ReportIntervalS+rand.Int63n(20)) * time.Second)
		case <-checkpointTicker.C:
			s.generateTasksAndExecute(ctx, tasks, proto.ShardTaskTypeCheckpoint, "do checkpoint")
		case <-trashShardCheckTicker.C:
			s.generateTasksAndExecute(ctx, tasks, proto.ShardTaskTypeCheckAndClear, "check shard and clear")
		case <-s.closer.Done():
			return
		}
	}
}

func (s *service) executeShardTask(ctx context.Context, task clustermgr.ShardTask, syncRun bool) error {
	span := trace.SpanFromContext(ctx)
	span.Infof("execute shard task:%+v", task)

	disk, err := s.getDisk(task.DiskID)
	if err != nil {
		return err
	}
	shard, err := disk.GetShardNoRWCheck(task.Suid)
	if err != nil {
		return err
	}

	var f func() error
	switch task.TaskType {
	case proto.ShardTaskTypeClearShard:
		f = func() error {
			_span, _ctx := trace.StartSpanFromContextWithTraceID(ctx, "", "delete-"+task.Suid.ToString())
			curVersion := shard.GetRouteVersion()
			if curVersion == task.RouteVersion {
				err = disk.DeleteShard(_ctx, task.Suid, task.RouteVersion)
				if err != nil {
					_span.Errorf("delete shard task[%+v] failed: %s", task, errors.Detail(err))
					return err
				}
				return nil
			}
			errMsg := fmt.Sprintf("route version not match, current: %d, task old: %d, task new: %d",
				curVersion, task.OldRouteVersion, task.RouteVersion)
			span.Errorf(errMsg)
			return errors.New(errMsg)
		}
	case proto.ShardTaskTypeSyncRouteVersion:
		f = func() error {
			_span, _ctx := trace.StartSpanFromContextWithTraceID(ctx, "", "update-version-"+task.Suid.ToString())
			curVersion := shard.GetRouteVersion()
			if curVersion < task.RouteVersion {
				err = disk.UpdateShardRouteVersion(_ctx, task.Suid, task.RouteVersion)
				if err != nil {
					_span.Errorf("update shard routeVersion task[%+v] failed: %s", task, err)
					return err
				}
				return nil
			}
			errMsg := fmt.Sprintf("route version not match, current: %d, task old: %d, task new: %d",
				curVersion, task.OldRouteVersion, task.RouteVersion)
			_span.Errorf(errMsg)
			return errors.New(errMsg)
		}
	case proto.ShardTaskTypeCheckAndClear:
		f = func() error {
			_span, _ctx := trace.StartSpanFromContextWithTraceID(ctx, "", "shard-check-"+task.Suid.ToString())
			if err := shard.CheckAndClearShard(_ctx); err != nil {
				_span.Errorf("check trash shard task[%+v] failed: %s", task, errors.Detail(err))
				return err
			}
			return nil
		}
	case proto.ShardTaskTypeCheckpoint:
		f = func() error {
			_span, _ctx := trace.StartSpanFromContextWithTraceID(ctx, "", "checkpoint-"+task.Suid.ToString())
			if err = shard.Checkpoint(_ctx); err != nil {
				_span.Errorf("shard do checkpoint task[%+v] failed: %s", task, errors.Detail(err))
				return err
			}
			return nil
		}
	default:
	}
	if f == nil {
		return nil
	}
	if syncRun {
		return f()
	}
	s.taskPool.Run(func() {
		f()
	})
	return nil
}

func (s *service) shardReports(ctx context.Context, shards []storage.ShardHandler, shardReports []clustermgr.ShardUnitInfo, sync bool, taskTypes ...proto.ShardTaskType) error {
	span, ctx := trace.StartSpanFromContext(ctx, "")
	disks := s.getAllDisks()
	readIndex := !sync
	for _, disk := range disks {
		shards = shards[:0]
		disk.RangeShardNoRWCheck(func(s storage.ShardHandler) bool {
			shards = append(shards, s)
			return true
		})
		for _, shard := range shards {
			stats, err := shard.Stats(ctx, readIndex)
			if err != nil {
				suid := shard.GetSuid()
				span.Errorf("get shard[%d] stat err: %s, suid[%d]", suid.ShardID(), err.Error(), suid)
				shardReports = append(shardReports, clustermgr.ShardUnitInfo{
					Suid:         shard.GetSuid(),
					DiskID:       disk.DiskID(),
					RouteVersion: shard.GetRouteVersion(),
				})
				continue
			}
			shardReports = append(shardReports, clustermgr.ShardUnitInfo{
				Suid:         stats.Suid,
				DiskID:       disk.DiskID(),
				AppliedIndex: stats.AppliedIndex,
				LeaderDiskID: stats.LeaderDiskID,
				Range:        stats.Range,
				RouteVersion: stats.RouteVersion,
			})
		}
	}

	tasks, err := s.transport.ShardReport(ctx, shardReports)
	if err != nil {
		span.Errorf("shard report failed: %s", err)
		return err
	}

	m := make(map[proto.ShardTaskType]struct{})
	for _, t := range taskTypes {
		m[t] = struct{}{}
	}

	for _, task := range tasks {
		// filter task
		if len(m) > 0 {
			if _, ok := m[task.TaskType]; !ok {
				continue
			}
		}

		if err := s.executeShardTask(ctx, task, sync); err != nil {
			span.Errorf("execute shard task[%+v] failed: %s", task, errors.Detail(err))
			return err
		}
	}
	return nil
}

func (s *service) generateTasksAndExecute(
	ctx context.Context,
	tasks []clustermgr.ShardTask,
	taskType proto.ShardTaskType, tag string,
) {
	span, ctx := trace.StartSpanFromContext(ctx, tag)

	disks := s.getAllDisks()
	tasks = tasks[:0]
	for _, disk := range disks {
		disk.RangeShard(func(s storage.ShardHandler) bool {
			tasks = append(tasks, clustermgr.ShardTask{
				TaskType: taskType,
				Suid:     s.GetSuid(),
				DiskID:   disk.DiskID(),
			})
			return true
		})
	}
	for _, task := range tasks {
		if err := s.executeShardTask(ctx, task, false); err != nil {
			span.Errorf("execute shard task[%+v] failed: %s", task, errors.Detail(err))
			continue
		}
	}
}
