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

package cacher

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/peterbourgon/diskv/v3"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func TestProxyCacherVolumeUpdate(t *testing.T) {
	c, cmCli, clean := newCacher(t, 2, nil)
	defer clean()
	cc := c.(*cacher)
	ctx := context.Background()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).Times(4)

	span := trace.SpanFromContextSafe(ctx)
	waitCached := func(vid proto.Vid) {
		require.Eventually(t, func() bool {
			return cc.getVolume(span, vid) != nil
		}, time.Second, 5*time.Millisecond)
	}

	// storeVolume writes disk synchronously, but withVolumeLock runs in a goroutine.
	// After the first GetVolume triggers a CM fetch, wait for the cache to be written
	// before subsequent requests can hit the cache.
	_, err := c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 1})
	require.NoError(t, err)
	waitCached(1)
	for range [99]struct{}{} {
		_, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}

	_, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 2})
	require.NoError(t, err)
	waitCached(2)
	for range [99]struct{}{} {
		_, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 2})
		require.NoError(t, err)
	}

	time.Sleep(time.Second * 4) // expired

	_, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 1})
	require.NoError(t, err)
	waitCached(1)
	for range [99]struct{}{} {
		_, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}

	_, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 2})
	require.NoError(t, err)
	waitCached(2)
	for range [99]struct{}{} {
		_, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 2})
		require.NoError(t, err)
	}
}

func TestProxyCacherVolumeFlush(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0, nil)
	defer clean()
	cc := c.(*cacher)
	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	volume := new(clustermgr.VolumeInfo)
	volume.Units = []clustermgr.Unit{{Vuid: 1234}, {Vuid: 5678}}
	version := proto.RouteVersion(12345678)
	volume.RouteVersion = version

	waitCached := func(vid proto.Vid) {
		require.Eventually(t, func() bool {
			return cc.getVolume(span, vid) != nil
		}, time.Second, 5*time.Millisecond)
	}

	// vid=1 first request hits CM; wait for cache write before subsequent requests
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(1)
	vol, err := c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 1})
	require.NoError(t, err)
	require.Equal(t, version, vol.RouteVersion)
	waitCached(1)
	for range [99]struct{}{} {
		vol, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
		require.Equal(t, version, vol.RouteVersion)
	}

	// Flush=true with Version=0: version check is skipped, every request goes to CM
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(100)
	for range [100]struct{}{} {
		vol, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 1, Flush: true})
		require.NoError(t, err)
		require.Equal(t, version, vol.RouteVersion)
	}

	// vid=3 first request has no cache, hits CM; Version=0x01 < cache version, subsequent requests skip CM
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(1)
	vol, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 3, Flush: true, Version: 0x01})
	require.NoError(t, err)
	require.Equal(t, version, vol.RouteVersion)
	waitCached(3)
	for range [99]struct{}{} {
		vol, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 3, Flush: true, Version: 0x01})
		require.NoError(t, err)
		require.Equal(t, version, vol.RouteVersion)
	}

	// vid=4 first request has no cache, hits CM; Version=version-1 < cache version, subsequent requests skip CM
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(1)
	vol, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 4, Flush: true, Version: uint64(version) - 1})
	require.NoError(t, err)
	require.Equal(t, version, vol.RouteVersion)
	waitCached(4)
	for range [99]struct{}{} {
		vol, err = c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 4, Flush: true, Version: uint64(version) - 1})
		require.NoError(t, err)
		require.Equal(t, version, vol.RouteVersion)
	}
}

func TestProxyCacherVolumeFlushSkipStaleCMVolume(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0, nil)
	defer clean()

	cc := c.(*cacher)
	newer := clustermgr.VolumeInfo{
		VolumeInfoBase: clustermgr.VolumeInfoBase{
			Vid:          1,
			RouteVersion: proto.RouteVersion(11),
		},
		Units: []clustermgr.Unit{{Vuid: proto.Vuid(1), DiskID: proto.DiskID(2), Host: "new-host"}},
	}
	cc.storeVolume(context.Background(), 1, cc.newExpiryVolume(newer))

	older := &clustermgr.VolumeInfo{
		VolumeInfoBase: clustermgr.VolumeInfoBase{
			Vid:          1,
			RouteVersion: proto.RouteVersion(10),
		},
		Units: []clustermgr.Unit{{Vuid: proto.Vuid(1), DiskID: proto.DiskID(1), Host: "old-host"}},
	}
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(older, nil).Times(1)

	// GetVolume now returns CM data directly (older); it no longer synchronously returns the newer cached value
	vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1, Flush: true})
	require.NoError(t, err)
	require.Equal(t, older.RouteVersion, vol.RouteVersion)

	// goroutine skips write because cached.RouteVersion(11) > cm.RouteVersion(10); cache retains newer
	span := trace.SpanFromContextSafe(context.Background())
	cached := cc.getVolume(span, proto.Vid(1))
	require.NotNil(t, cached)
	require.Equal(t, newer.RouteVersion, cached.RouteVersion)
	require.Equal(t, newer.Units, cached.Units)
}

func TestProxyCacherVolumeSingle(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0, nil)
	defer clean()

	done := make(chan struct{})
	cmCli.EXPECT().GetVolumeInfo(A, A).DoAndReturn(
		func(_ context.Context, _ *clustermgr.GetVolumeArgs) (*clustermgr.VolumeInfo, error) {
			<-done
			return &clustermgr.VolumeInfo{}, nil
		})

	var wg sync.WaitGroup
	const n = _defaultClustermgrConcurrency
	wg.Add(n)
	for range [n]struct{}{} {
		go func() {
			c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
			wg.Done()
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
}

func TestProxyCacherVolumeError(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0, nil)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(nil, errors.New("mock error")).Times(1)
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(nil, errcode.ErrVolumeNotExist).Times(2)

	_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
	require.Error(t, err)
	_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 2, Flush: true})
	require.ErrorIs(t, err, errcode.ErrVolumeNotExist)
	_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1, Flush: false})
	require.ErrorIs(t, err, errcode.ErrVolumeNotExist)
}

func TestProxyCacherVolumeCacheMiss(t *testing.T) {
	c, cmCli, clean := newCacher(t, 2, nil)
	defer clean()

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).Times(3)
	_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
	require.NoError(t, err)
	cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil).AnyTimes()
	<-c.(*cacher).syncChan

	basePath := c.(*cacher).config.DiskvBasePath
	{ // memory cache miss, load from diskv
		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, VolumeExpirationS: 2}, cmCli)
		_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}
	{ // cannot decode diskv value
		file, err := os.OpenFile(c.(*cacher).DiskvFilename(diskvKeyVolume(1)), os.O_RDWR, 0o644)
		require.NoError(t, err)
		file.Write([]byte("}}}}}"))
		file.Close()

		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, VolumeExpirationS: 2}, cmCli)
		_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}
	{ // load diskv expired
		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, VolumeExpirationS: 2}, cmCli)
		time.Sleep(time.Second * 3)
		_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}
}

func TestProxyCacherVolumeMemExpiredSkipsDiskv(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0, nil)
	defer clean()
	cc := c.(*cacher)
	ctx := context.Background()

	info := clustermgr.VolumeInfo{
		VolumeInfoBase: clustermgr.VolumeInfoBase{Vid: 1, RouteVersion: proto.RouteVersion(10)},
	}
	vol := cc.newExpiryVolume(info)
	cc.storeVolume(ctx, 1, vol)
	<-cc.syncChan

	// Expire the in-memory entry without touching diskv, so diskv still holds valid data.
	// This isolates the "mem expired -> skip diskv -> go to CM" path.
	vol.expiration = time.Nanosecond

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&info, nil).Times(1)
	_, err := c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: 1})
	require.NoError(t, err)
}

func BenchmarkProxyMemoryHit(b *testing.B) {
	c, cmCli, clean := newCacher(b, 0, nil)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).AnyTimes()

	ctx := context.Background()
	args := &proxy.CacheVolumeArgs{Vid: 1}
	_, err := c.GetVolume(ctx, args)
	require.NoError(b, err)

	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		c.GetVolume(ctx, args)
	}
}

func BenchmarkProxyDiskvHit(b *testing.B) {
	c, cmCli, clean := newCacher(b, 0, nil)
	defer clean()
	c.(*cacher).diskv.AdvancedTransform = func(s string) *diskv.PathKey {
		return &diskv.PathKey{Path: proxy.DiskvPathTransform(s), FileName: diskvKeyVolume(1)}
	}
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).AnyTimes()

	ctx := context.Background()
	args := &proxy.CacheVolumeArgs{}
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		args.Vid = proto.Vid(ii)
		c.GetVolume(ctx, args)
	}
}

func BenchmarkProxyDiskvMiss(b *testing.B) {
	c, cmCli, clean := newCacher(b, 0, nil)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).AnyTimes()

	ctx := context.Background()
	args := &proxy.CacheVolumeArgs{}
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		args.Vid = proto.Vid(ii)
		c.GetVolume(ctx, args)
	}
}

func BenchmarkProxyClusterMiss(b *testing.B) {
	c, cmCli, clean := newCacher(b, 0, nil)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(nil, errcode.ErrVolumeNotExist).AnyTimes()

	ctx := context.Background()
	args := &proxy.CacheVolumeArgs{Vid: 1}
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		args.Vid = proto.Vid(ii)
		c.GetVolume(ctx, args)
	}
}

func TestProxyCacheCloser(t *testing.T) {
	c, _, clean := newCacher(t, 0, nil)
	defer clean()
	closer.Close(c)
	require.True(t, c.(closer.Closer).IsClosed())
}

func TestVolumeRouteSyncAddVolume(t *testing.T) {
	cmCli := mocks.NewMockClientAPI(C(t))
	// CM fills VolumeInfoBase.RouteVersion with the latest overall version (20),
	// but item.RouteVersion is the version of this specific route record (10).
	// The proxy must use item.RouteVersion (10) for the cached volume, not 20.
	payload := &clustermgr.RouteItemAddVolume{
		Vid: 1,
		Units: []clustermgr.VolumeUnitInfoBase{
			{Vuid: proto.Vuid(1), DiskID: proto.DiskID(1), Host: "host-1"},
		},
		VolumeInfoBase: clustermgr.VolumeInfoBasePB{
			Vid:          1,
			CodeMode:     codemode.EC6P6,
			RouteVersion: proto.RouteVersion(20),
		},
	}
	cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
		RouteVersion: proto.RouteVersion(10),
		Items: []clustermgr.VolumeRouteItem{{
			RouteVersion: proto.RouteVersion(10),
			Type:         proto.RouteItemTypeAddVolume,
			Item:         mustMarshalAny(t, payload),
		}},
	}, nil).AnyTimes()

	c, _, clean := newCacher(t, 0, cmCli)
	defer clean()
	cc := c.(*cacher)

	require.NoError(t, cc.syncVolumeRoutes(context.Background()))
	require.Equal(t, proto.RouteVersion(10), cc.getVolRouteVersion())
	data, err := cc.diskv.Read(diskvKeyVolumeRouteVersion)
	require.NoError(t, err)
	require.Equal(t, "10", string(data))

	span := trace.SpanFromContextSafe(context.Background())
	vol := cc.getVolume(span, proto.Vid(1))
	require.NotNil(t, vol)
	// must be item.RouteVersion (10), not payload.VolumeInfoBase.RouteVersion (20)
	require.Equal(t, proto.RouteVersion(10), vol.VolumeInfo.RouteVersion)
	require.Len(t, vol.VolumeInfo.Units, 1)
	require.Equal(t, proto.DiskID(1), vol.VolumeInfo.Units[0].DiskID)
}

func TestProxyCacherVolumeFlushVersionComparison(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0, nil)
	defer clean()
	cc := c.(*cacher)

	cached := clustermgr.VolumeInfo{
		VolumeInfoBase: clustermgr.VolumeInfoBase{Vid: 1, RouteVersion: proto.RouteVersion(10)},
		Units:          []clustermgr.Unit{{Vuid: proto.Vuid(1), DiskID: proto.DiskID(1)}},
	}
	cc.storeVolume(context.Background(), 1, cc.newExpiryVolume(cached))
	// drain the syncChan signal from setup storeVolume
	select {
	case <-cc.syncChan:
	default:
	}

	newer := &clustermgr.VolumeInfo{
		VolumeInfoBase: clustermgr.VolumeInfoBase{Vid: 1, RouteVersion: proto.RouteVersion(11)},
		Units:          []clustermgr.Unit{{Vuid: proto.Vuid(1), DiskID: proto.DiskID(2)}},
	}

	// cache ver=10 < access.Version=11: proxy is stale, should trigger a CM fetch
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(newer, nil).Times(1)
	vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1, Flush: true, Version: 11})
	require.NoError(t, err)
	require.Equal(t, proto.RouteVersion(11), vol.RouteVersion)

	// wait for the async goroutine to write ver=11 into cache
	<-cc.syncChan

	span := trace.SpanFromContextSafe(context.Background())

	// cache ver=11 == access.Version=11: skip CM
	vol, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1, Flush: true, Version: 11})
	require.NoError(t, err)
	require.Equal(t, proto.RouteVersion(11), vol.RouteVersion)
	require.Equal(t, proto.RouteVersion(11), cc.getVolume(span, 1).RouteVersion)

	// cache ver=11 > access.Version=10: proxy is ahead, skip CM
	vol, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1, Flush: true, Version: 10})
	require.NoError(t, err)
	require.Equal(t, proto.RouteVersion(11), vol.RouteVersion)
}

func TestVolumeRouteSyncUpdateMultipleUnits(t *testing.T) {
	cmCli := mocks.NewMockClientAPI(C(t))

	vid := proto.Vid(1)
	// CM fills VolumeInfoBase.RouteVersion with the latest overall version (12) for all items,
	// but each item carries its own item.RouteVersion (10, 11, 12 respectively).
	addPayload := &clustermgr.RouteItemAddVolume{
		Vid: vid,
		Units: []clustermgr.VolumeUnitInfoBase{
			{Vuid: proto.EncodeVuid(proto.EncodeVuidPrefix(vid, 0), 1), DiskID: proto.DiskID(1), Host: "host-1"},
			{Vuid: proto.EncodeVuid(proto.EncodeVuidPrefix(vid, 1), 1), DiskID: proto.DiskID(2), Host: "host-2"},
		},
		VolumeInfoBase: clustermgr.VolumeInfoBasePB{Vid: vid, RouteVersion: proto.RouteVersion(12)},
	}
	// CM behavior: two UpdateVolume items for the same volume, both with payload.VolumeInfoBase.RouteVersion=12 (current latest).
	// Correct idempotency must rely on item.RouteVersion (11, 12), not the version inside the payload.
	update0 := &clustermgr.RouteItemUpdateVolume{
		Vid:            vid,
		Unit:           clustermgr.VolumeUnitInfoBase{Vuid: proto.EncodeVuid(proto.EncodeVuidPrefix(vid, 0), 2), DiskID: proto.DiskID(3), Host: "host-3"},
		VolumeInfoBase: clustermgr.VolumeInfoBasePB{Vid: vid, RouteVersion: proto.RouteVersion(12)},
	}
	update1 := &clustermgr.RouteItemUpdateVolume{
		Vid:            vid,
		Unit:           clustermgr.VolumeUnitInfoBase{Vuid: proto.EncodeVuid(proto.EncodeVuidPrefix(vid, 1), 2), DiskID: proto.DiskID(4), Host: "host-4"},
		VolumeInfoBase: clustermgr.VolumeInfoBasePB{Vid: vid, RouteVersion: proto.RouteVersion(12)},
	}

	gomock.InOrder(
		// New calls syncVolumeRoutes once on init when version=0
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil),
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
			RouteVersion: proto.RouteVersion(10),
			Items: []clustermgr.VolumeRouteItem{{
				RouteVersion: proto.RouteVersion(10),
				Type:         proto.RouteItemTypeAddVolume,
				Item:         mustMarshalAny(t, addPayload),
			}},
		}, nil),
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
			RouteVersion: proto.RouteVersion(12),
			Items: []clustermgr.VolumeRouteItem{
				{RouteVersion: proto.RouteVersion(11), Type: proto.RouteItemTypeUpdateVolume, Item: mustMarshalAny(t, update0)},
				{RouteVersion: proto.RouteVersion(12), Type: proto.RouteItemTypeUpdateVolume, Item: mustMarshalAny(t, update1)},
			},
		}, nil),
	)

	c, _, clean := newCacher(t, 0, cmCli)
	defer clean()
	cc := c.(*cacher)

	require.NoError(t, cc.syncVolumeRoutes(context.Background()))
	require.NoError(t, cc.syncVolumeRoutes(context.Background()))
	require.Equal(t, proto.RouteVersion(12), cc.getVolRouteVersion())

	span := trace.SpanFromContextSafe(context.Background())
	vol := cc.getVolume(span, vid)
	require.NotNil(t, vol)
	require.Equal(t, proto.RouteVersion(12), vol.VolumeInfo.RouteVersion)
	require.Len(t, vol.VolumeInfo.Units, 2)

	unitByIndex := func(units []clustermgr.Unit, idx uint8) *clustermgr.Unit {
		for i := range units {
			if units[i].Vuid.Index() == idx {
				return &units[i]
			}
		}
		return nil
	}
	u0 := unitByIndex(vol.VolumeInfo.Units, 0)
	require.NotNil(t, u0)
	require.Equal(t, proto.DiskID(3), u0.DiskID, "unit-0 should be updated to disk3")
	u1 := unitByIndex(vol.VolumeInfo.Units, 1)
	require.NotNil(t, u1)
	require.Equal(t, proto.DiskID(4), u1.DiskID, "unit-1 should be updated to disk4")
}

func TestVolumeRouteSyncUpdateUnit(t *testing.T) {
	cmCli := mocks.NewMockClientAPI(C(t))
	addPayload := &clustermgr.RouteItemAddVolume{
		Vid: 1,
		Units: []clustermgr.VolumeUnitInfoBase{
			{Vuid: proto.Vuid(1), DiskID: proto.DiskID(1), Host: "host-1"},
			{Vuid: proto.Vuid(2), DiskID: proto.DiskID(2), Host: "host-2"},
		},
		VolumeInfoBase: clustermgr.VolumeInfoBasePB{
			Vid:          1,
			CodeMode:     codemode.EC6P6,
			RouteVersion: proto.RouteVersion(10),
		},
	}
	// CM fills VolumeInfoBase.RouteVersion with the latest overall version (20),
	// but item.RouteVersion is the version of this specific route record (11).
	// The proxy must use item.RouteVersion (11) for the cached volume, not 20.
	updatePayload := &clustermgr.RouteItemUpdateVolume{
		Vid: 1,
		Unit: clustermgr.VolumeUnitInfoBase{
			Vuid:   proto.EncodeVuid(proto.VuidPrefix(2), 1),
			DiskID: proto.DiskID(3),
			Host:   "host-3",
		},
		VolumeInfoBase: clustermgr.VolumeInfoBasePB{
			Vid:          1,
			CodeMode:     codemode.EC6P6,
			RouteVersion: proto.RouteVersion(20),
		},
	}

	gomock.InOrder(
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil),
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
			RouteVersion: proto.RouteVersion(10),
			Items: []clustermgr.VolumeRouteItem{{
				RouteVersion: proto.RouteVersion(10),
				Type:         proto.RouteItemTypeAddVolume,
				Item:         mustMarshalAny(t, addPayload),
			}},
		}, nil),
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
			RouteVersion: proto.RouteVersion(11),
			Items: []clustermgr.VolumeRouteItem{{
				RouteVersion: proto.RouteVersion(11),
				Type:         proto.RouteItemTypeUpdateVolume,
				Item:         mustMarshalAny(t, updatePayload),
			}},
		}, nil),
	)

	c, _, clean := newCacher(t, 0, cmCli)
	defer clean()
	cc := c.(*cacher)

	require.NoError(t, cc.syncVolumeRoutes(context.Background()))
	require.NoError(t, cc.syncVolumeRoutes(context.Background()))
	require.Equal(t, proto.RouteVersion(11), cc.getVolRouteVersion())

	span := trace.SpanFromContextSafe(context.Background())
	vol := cc.getVolume(span, proto.Vid(1))
	require.NotNil(t, vol)
	// must be item.RouteVersion (11), not payload.VolumeInfoBase.RouteVersion (20)
	require.Equal(t, proto.RouteVersion(11), vol.VolumeInfo.RouteVersion)
	require.Len(t, vol.VolumeInfo.Units, 2)
	require.Equal(t, proto.DiskID(3), vol.VolumeInfo.Units[0].DiskID)
	require.Equal(t, "host-3", vol.VolumeInfo.Units[0].Host)
}

func TestVolumeRouteSyncNoChange(t *testing.T) {
	cmCli := mocks.NewMockClientAPI(C(t))
	cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil).AnyTimes()
	c, _, clean := newCacher(t, 0, cmCli)
	defer clean()
	require.NoError(t, c.(*cacher).syncVolumeRoutes(context.Background()))
	require.Equal(t, proto.RouteVersion(0), c.(*cacher).getVolRouteVersion())
}

func TestVolumeRouteSyncApplyFailed(t *testing.T) {
	cmCli := mocks.NewMockClientAPI(C(t))
	gomock.InOrder(
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil),
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
			RouteVersion: proto.RouteVersion(11),
			Items: []clustermgr.VolumeRouteItem{{
				RouteVersion: proto.RouteVersion(11),
				Type:         proto.RouteItemTypeAddVolume,
				Item:         nil,
			}},
		}, nil),
	)

	c, _, clean := newCacher(t, 0, cmCli)
	defer clean()
	cc := c.(*cacher)

	err := cc.syncVolumeRoutes(context.Background())
	require.Error(t, err)
	require.Equal(t, proto.RouteVersion(0), cc.getVolRouteVersion())
}

func TestVolumeRouteVersionPersist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cmCli := mocks.NewMockClientAPI(ctrl)
	cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil).AnyTimes()

	basePath := path.Join(os.TempDir(), "proxy-cacher", fmt.Sprintf("persist-%d", rand.Int()))
	defer os.RemoveAll(basePath)

	cfg := ConfigCache{
		DiskvBasePath: basePath,
	}
	c1, err := New(1, cfg, cmCli)
	require.NoError(t, err)
	cc1 := c1.(*cacher)
	require.NoError(t, cc1.persistVolRouteVersion(context.Background(), proto.RouteVersion(99)))
	cc1.setVolRouteVersion(proto.RouteVersion(99))

	c2, err := New(1, cfg, cmCli)
	require.NoError(t, err)
	cc2 := c2.(*cacher)
	require.Equal(t, proto.RouteVersion(99), cc2.getVolRouteVersion())
}

func mustMarshalAny(t *testing.T, msg gogoproto.Message) *types.Any {
	t.Helper()
	any, err := types.MarshalAny(msg)
	require.NoError(t, err)
	return any
}
