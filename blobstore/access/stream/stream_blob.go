package stream

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

func (h *Handler) GetBlob(ctx context.Context, args *acapi.GetBlobArgs) (*proto.Location, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("get blob args:%+v", *args)

	var blob shardnode.GetBlobRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      args.Mode,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err // not retry
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		blob, err = h.shardnodeClient.GetBlob(ctx, host, shardnode.GetBlobArgs{
			Header: header,
			Name:   args.BlobName,
		})
		if err != nil {
			return h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				mode:          args.Mode,
				err:           err,
			})
		}

		return true, nil
	})

	if rerr != nil {
		span.Errorf("get blob failed, args:%+v, err:%+v", *args, rerr)
	}
	return &blob.Blob.Location, rerr
}

func (h *Handler) CreateBlob(ctx context.Context, args *acapi.CreateBlobArgs) (*proto.Location, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("create blob args:%+v", *args)

	err := h.fixCreateBlobArgs(ctx, args)
	if err != nil {
		return nil, err
	}

	var blob shardnode.CreateBlobRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      acapi.GetShardModeLeader,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		blob, err = h.shardnodeClient.CreateBlob(ctx, host, shardnode.CreateBlobArgs{
			Header:    header,
			Name:      args.BlobName,
			CodeMode:  args.CodeMode,
			Size_:     args.Size,
			SliceSize: args.SliceSize,
		})
		if err != nil {
			return h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				mode:          acapi.GetShardModeLeader,
				err:           err,
			})
		}
		return true, nil
	})

	if rerr != nil {
		span.Errorf("create blob failed, args:%+v, err:%+v", *args, rerr)
	}
	return &blob.Blob.Location, rerr
}

func (h *Handler) DeleteBlob(ctx context.Context, args *acapi.DelBlobArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("delete blob args:%+v", *args)

	var blob shardnode.GetBlobRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      acapi.GetShardModeLeader,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		blob, err = h.shardnodeClient.FindAndDeleteBlob(ctx, host, shardnode.DeleteBlobArgs{
			Header: header,
			Name:   args.BlobName,
		})
		if err != nil {
			return h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				mode:          acapi.GetShardModeLeader,
				err:           err,
			})
		}

		return true, nil
	})

	if rerr != nil {
		span.Errorf("delete blob failed, args:%+v, err:%+v", *args, rerr)
		return rerr
	}

	return h.Delete(ctx, &blob.Blob.Location)
}

func (h *Handler) SealBlob(ctx context.Context, args *acapi.SealBlobArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("seal blob args:%+v", *args)

	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      acapi.GetShardModeLeader,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		err = h.shardnodeClient.SealBlob(ctx, host, shardnode.SealBlobArgs{
			Header: header,
			Name:   args.BlobName,
			Size_:  args.Size,
			Slices: args.Slices,
		})
		if err != nil {
			return h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				mode:          acapi.GetShardModeLeader,
				err:           err,
			})
		}
		return true, nil
	})

	if rerr != nil {
		span.Errorf("seal blob failed, args:%+v, err:%+v", *args, rerr)
	}
	return rerr
}

func (h *Handler) ListBlob(ctx context.Context, args *acapi.ListBlobArgs) (ret shardnode.ListBlobRet, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("list blob args:%+v", *args)
	defer func() {
		if err != nil {
			span.Errorf("list blob failed, args:%+v, err:%+v", *args, err)
		}
	}()

	if args.ShardID != 0 {
		return h.listSpecificShard(ctx, args)
	}

	return h.listManyShards(ctx, args)
}

func (h *Handler) AllocSlice(ctx context.Context, args *acapi.AllocSliceArgs) (shardnode.AllocSliceRet, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("alloc blob args:%+v", *args)

	var slices shardnode.AllocSliceRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      acapi.GetShardModeLeader,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		slices, err = h.shardnodeClient.AllocSlice(ctx, host, shardnode.AllocSliceArgs{
			Header:      header,
			Name:        args.BlobName,
			CodeMode:    args.CodeMode,
			Size_:       args.Size,
			FailedSlice: args.FailSlice,
		})
		if err != nil {
			return h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				mode:          acapi.GetShardModeLeader,
				err:           err,
			})
		}
		return true, nil
	})

	if rerr != nil {
		span.Errorf("alloc slice failed, args:%+v, err:%+v", *args, rerr)
	}
	return slices, rerr
}

func (h *Handler) listSpecificShard(ctx context.Context, args *acapi.ListBlobArgs) (shardnode.ListBlobRet, error) {
	span := trace.SpanFromContextSafe(ctx)
	var ret shardnode.ListBlobRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getOpHeaderByID(ctx, args.ClusterID, args.ShardID, args.Mode)
		if err != nil {
			return true, err
		}

		interrupt := false
		ret, interrupt, err = h.listSingleShardEnough(ctx, args, header)
		span.Debugf("list blob, shardID=%d, interrupt:%t, length:%d, err:%+v", args.ShardID, interrupt, len(ret.Blobs), err)

		if err != nil {
			return interrupt, err
		}
		return true, nil
	})
	return ret, rerr
}

func (h *Handler) listManyShards(ctx context.Context, args *acapi.ListBlobArgs) (shardnode.ListBlobRet, error) {
	span := trace.SpanFromContextSafe(ctx)
	shardMgr, err := h.clusterController.GetShardController(args.ClusterID)
	if err != nil {
		return shardnode.ListBlobRet{}, err
	}

	var (
		shard   controller.Shard
		allBlob shardnode.ListBlobRet
	)
	if len(args.Marker) == 0 {
		shard, err = shardMgr.GetFisrtShard(ctx)
		if err != nil {
			return shardnode.ListBlobRet{}, err
		}
	} else {
		unionMarker := acapi.ListBlobEncodeMarker{}
		if err = unionMarker.Unmarshal(args.Marker); err != nil {
			return shardnode.ListBlobRet{}, fmt.Errorf("fail to unmarshal marker, err: %+v", err)
		}
		allBlob.NextMarker = unionMarker.Marker
		shard, err = shardMgr.GetShardByRange(ctx, unionMarker.Range)
		if err != nil {
			return shardnode.ListBlobRet{}, err
		}
		span.Debugf("list blob at multi shards, prefix=%s, range=%s, marker=%s", string(args.Prefix), unionMarker.Range.String(), unionMarker.Marker)
	}

	lastRange := shard.GetRange()
	count := int64(args.Count)
	for count > 0 {
		var ret shardnode.ListBlobRet
		interrupt := false
		rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
			header, err := h.getOpHeaderByShard(ctx, shardMgr, shard, args.Mode, nil)
			if err != nil {
				return interrupt, err
			}
			args.Marker = allBlob.NextMarker
			args.Count = uint64(count)
			ret, interrupt, err = h.listSingleShardEnough(ctx, args, header)
			span.Debugf("list blob, shardID=%d, interrupt:%t, length:%d, err:%+v", args.ShardID, interrupt, len(ret.Blobs), err)

			if err != nil {
				return interrupt, err
			}
			return true, nil
		})
		if rerr != nil {
			return shardnode.ListBlobRet{}, rerr
		}

		allBlob.Blobs = append(allBlob.Blobs, ret.Blobs...)
		allBlob.NextMarker = ret.NextMarker
		count -= int64(len(ret.Blobs))
		if ret.NextMarker == nil {
			shard, err = shardMgr.GetNextShard(ctx, lastRange)
			if err != nil {
				return shardnode.ListBlobRet{}, err
			}
			// err == nil && shard == nil, means last shard, reach end
			if shard == nil {
				lastRange = sharding.Range{}
				break // reach end
			}
			lastRange = shard.GetRange()
		}
	}

	// reach end, don't need marshal
	if len(allBlob.NextMarker) == 0 && lastRange.Type == 0 {
		return allBlob, nil
	}

	markers := acapi.ListBlobEncodeMarker{
		Range:  lastRange,          // empty, means reach the end; else, means next expect shard
		Marker: allBlob.NextMarker, // empty, means current shard list end; else, means expect begin blob name
	}
	unionMarker, err := markers.Marshal()
	allBlob.NextMarker = unionMarker
	return allBlob, err
}

func (h *Handler) listSingleShardEnough(ctx context.Context, args *acapi.ListBlobArgs, header shardnode.ShardOpHeader) (shardnode.ListBlobRet, bool, error) {
	host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
	if err != nil {
		return shardnode.ListBlobRet{}, true, err
	}

	ret, err := h.shardnodeClient.ListBlob(ctx, host, shardnode.ListBlobArgs{
		Header: header,
		Prefix: args.Prefix,
		Marker: args.Marker,
		Count:  args.Count,
	})
	if err != nil {
		interrupt, err1 := h.punishAndUpdate(ctx, &punishArgs{
			ShardOpHeader: header,
			clusterID:     args.ClusterID,
			host:          host,
			mode:          args.Mode,
			err:           err,
		})
		return shardnode.ListBlobRet{}, interrupt, err1
	}

	return ret, true, nil
}

func (h *Handler) getShardOpHeader(ctx context.Context, args *acapi.GetShardCommonArgs) (shardnode.ShardOpHeader, error) {
	shardMgr, err := h.clusterController.GetShardController(args.ClusterID)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	if args.ShardKeys == nil {
		args.ShardKeys = [][]byte{args.BlobName}
	}
	shard, err := shardMgr.GetShard(ctx, args.ShardKeys)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	oh, err := h.getOpHeaderByShard(ctx, shardMgr, shard, args.Mode, args.ShardKeys)
	return oh, err
}

func (h *Handler) getOpHeaderByID(ctx context.Context, clusterID proto.ClusterID, shardID proto.ShardID, mode acapi.GetShardMode) (shardnode.ShardOpHeader, error) {
	shardMgr, err := h.clusterController.GetShardController(clusterID)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	shard, err := shardMgr.GetShardByID(ctx, shardID)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	return h.getOpHeaderByShard(ctx, shardMgr, shard, mode, nil)
}

func (h *Handler) getOpHeaderByShard(ctx context.Context, shardMgr controller.IShardController, shard controller.Shard,
	mode acapi.GetShardMode, shardKeys [][]byte,
) (shardnode.ShardOpHeader, error) {
	span := trace.SpanFromContextSafe(ctx)

	spaceID := shardMgr.GetSpaceID()
	info, err := shard.GetMember(ctx, mode, 0)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	oh := shardnode.ShardOpHeader{
		SpaceID:      spaceID,
		DiskID:       info.DiskID,
		Suid:         info.Suid,
		RouteVersion: info.RouteVersion,
		ShardKeys:    shardKeys, // don't need shardKeys when list blob, other required
	}

	span.Debugf("shard op header: %+v", oh)
	return oh, nil
}

func (h *Handler) getShardHost(ctx context.Context, clusterID proto.ClusterID, diskID proto.DiskID) (string, error) {
	span := trace.SpanFromContextSafe(ctx)
	s, err := h.clusterController.GetServiceController(clusterID)
	if err != nil {
		return "", err
	}

	hostInfo, err := s.GetShardnodeHost(ctx, diskID)
	if err != nil {
		return "", err
	}

	span.Debugf("get shard host:%+v", *hostInfo)
	return hostInfo.Host, nil
}

type punishArgs struct {
	shardnode.ShardOpHeader
	clusterID proto.ClusterID
	host      string
	mode      acapi.GetShardMode
	err       error
}

func (h *Handler) punishAndUpdate(ctx context.Context, args *punishArgs) (bool, error) {
	span := trace.SpanFromContextSafe(ctx)

	// This error is coming from the shardnode interface, and we want to make sure that the error can be parsed into an error code
	code := rpc.DetectStatusCode(args.err)

	// leader disk status: normal->EIO->broken->repairing->repaired. it greater than broken will mark punished
	// if bad disk(punished), we select another disk as leader; old leader is not in disk units, after update route(replace suid index unit)
	// and then call sn return NoLeader, and we fetch and update new leader shard
	//    1. old leader: repaired(eio, or status >= broken), sn will remove disk and return DiskNotFound, update route
	//    2. old leader: broken(eio, broken, repairing), sn return DiskBroken
	// cm catalog units is always correct, but its leaderDiskID may be wrong
	switch code {
	case errcode.CodeDiskBroken: // read shard at bad disk, but shard/disk is reparing
		// if follow node broken disk, it will not election, just try again, change other shard;
		// if leader node broken disk, it cant get shard stats, wait new leader
		h.punishShardnodeDisk(ctx, args.clusterID, args.DiskID, args.host, "Broken")
		if args.mode == acapi.GetShardModeLeader {
			err1 := h.waitShardnodeNextLeader(ctx, args.clusterID, args.Suid, args.DiskID)
			if err1 != nil {
				span.Warnf("fail to change other shard node, cluster:%d, err:%+v", args.clusterID, err1)
			}
		}
		return false, args.err

		// update route and punish
	case errcode.CodeShardNodeDiskNotFound: // read shard at bad disk, but old broken disk is repaired, all shard repaired
		h.punishShardnodeDisk(ctx, args.clusterID, args.DiskID, args.host, "NotFound")
		if err1 := h.updateShardRoute(ctx, args.clusterID); err1 != nil {
			span.Warnf("fail to update shard route, cluster:%d, err:%+v", args.clusterID, err1)
		}
		return false, args.err

		// update route
	case errcode.CodeShardDoesNotExist, // intermediate state disk, not a final state; shard is removed, disk is repairing/repaired ; suid not match disk id
		errcode.CodeShardRouteVersionNeedUpdate: // header op version less than shardnode version
		if err1 := h.updateShardRoute(ctx, args.clusterID); err1 != nil {
			span.Warnf("fail to update shard route, cluster:%d, err:%+v", args.clusterID, err1)
		}
		return false, args.err

		// select master
	case errcode.CodeShardNodeNotLeader: // leader disk id error when create/delete/seal
		if err1 := h.updateShard(ctx, args); err1 != nil {
			span.Warnf("fail to update shard, cluster:%d, err:%+v", args.clusterID, err1)
		}
		return false, args.err

	default:
	}

	// err:dial tcp 127.0.0.1:9100: connect: connection refused  ； code:500
	if errorConnectionRefused(args.err) {
		span.Warnf("shardnode connection refused, args:%+v, err:%+v", *args, args.err)
		h.groupRun.Do("shardnode-leader-"+args.DiskID.ToString(), func() (interface{}, error) {
			// must wait have master leader, block wait
			h.punishShardnodeDisk(ctx, args.clusterID, args.DiskID, args.host, "Refused")
			err1 := h.waitShardnodeNextLeader(ctx, args.clusterID, args.Suid, args.DiskID)
			if err1 != nil {
				span.Warnf("fail to change other shard node, cluster:%d, err:%+v", args.clusterID, err1)
			}
			return nil, err1
		})
		return false, errcode.ErrConnectionRefused
	}

	// eio or other error; if shardNode restarts quickly so wait for it to start, and try again
	return false, args.err
}

func (h *Handler) updateShardRoute(ctx context.Context, clusterID proto.ClusterID) error {
	shardMgr, err := h.clusterController.GetShardController(clusterID)
	if err != nil {
		return err
	}

	return shardMgr.UpdateRoute(ctx)
}

func (h *Handler) updateShard(ctx context.Context, args *punishArgs) error {
	shardMgr, err := h.clusterController.GetShardController(args.clusterID)
	if err != nil {
		return err
	}

	shardStat, err := h.getLeaderShardInfo(ctx, args.clusterID, args.host, args.DiskID, args.Suid, 0)
	if err != nil {
		return err
	}

	return shardMgr.UpdateShard(ctx, shardStat)
}

func (h *Handler) waitShardnodeNextLeader(ctx context.Context, clusterID proto.ClusterID, suid proto.Suid, badDisk proto.DiskID) error {
	shardMgr, err := h.clusterController.GetShardController(clusterID)
	if err != nil {
		return err
	}
	shard, err := shardMgr.GetShardByID(ctx, suid.ShardID())
	if err != nil {
		return err
	}

	// we get new disk, exclude bad diskID
	newDisk, err := shard.GetMember(ctx, acapi.GetShardModeRandom, badDisk)
	if err != nil {
		return err
	}
	newHost, err := h.getShardHost(ctx, clusterID, newDisk.DiskID)
	if err != nil {
		return err
	}
	// span := trace.SpanFromContextSafe(ctx)
	// span.Debugf("get newDisk:%+v, old host:%s, old disk:%d", newDisk, args.host, args.DiskID)

	shardStat, err := h.getLeaderShardInfo(ctx, clusterID, newHost, newDisk.DiskID, newDisk.Suid, badDisk)
	if err != nil {
		return err
	}

	return shardMgr.UpdateShard(ctx, shardStat)
}

func (h *Handler) getLeaderShardInfo(ctx context.Context, clusterID proto.ClusterID, host string, diskID proto.DiskID, suid proto.Suid, badDisk proto.DiskID) (shardnode.ShardStats, error) {
	span := trace.SpanFromContextSafe(ctx)

	for i := 0; i < h.ShardnodeRetryTimes; i++ {
		// 1. get leader info
		leader, err := h.shardnodeClient.GetShardStats(ctx, host, shardnode.GetShardArgs{
			DiskID: diskID,
			Suid:   suid,
		})
		if err != nil {
			if code := rpc.DetectStatusCode(err); code == errcode.CodeShardNoLeader {
				span.Warnf("shard node is in the election, host:%s, disk:%d, suid:%d, badDisk:%d", host, diskID, suid, badDisk)
				time.Sleep(time.Millisecond * time.Duration(h.ShardnodeRetryIntervalMS))
				continue
			}
			return shardnode.ShardStats{}, err
		}

		// skip bad host. LeaderDiskID is 0 means in the election. bad disk is last leader, not start election yet
		if leader.LeaderDiskID == 0 || leader.LeaderDiskID == badDisk {
			span.Warnf("shard node is in the election, host:%s, disk:%d, suid:%d, badDisk:%d", host, diskID, suid, badDisk)
			time.Sleep(time.Millisecond * time.Duration(h.ShardnodeRetryIntervalMS))
			continue
		}

		return leader, nil
	}

	return shardnode.ShardStats{}, errcode.ErrShardNoLeader
}

func (h *Handler) fixCreateBlobArgs(ctx context.Context, args *acapi.CreateBlobArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	if int64(args.Size) > h.maxObjectSize {
		span.Info("exceed max object size", h.maxObjectSize)
		return errcode.ErrAccessExceedSize
	}

	if args.SliceSize == 0 {
		args.SliceSize = atomic.LoadUint32(&h.MaxBlobSize)
		span.Debugf("fill slice size:%d", args.SliceSize)
	}

	if args.CodeMode == 0 {
		args.CodeMode = h.allCodeModes.SelectCodeMode(int64(args.Size))
		span.Debugf("select codemode:%d", args.CodeMode)
	}

	if !args.CodeMode.IsValid() {
		span.Infof("invalid codemode:%d", args.CodeMode)
		return errcode.ErrIllegalArguments
	}

	if args.ClusterID == 0 {
		cluster, err := h.clusterController.ChooseOne()
		if err != nil {
			return err
		}
		args.ClusterID = cluster.ClusterID
		span.Debugf("choose cluster[%+v]", cluster)
	}

	return nil
}
