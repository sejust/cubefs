package base

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type routeStorageMock struct {
	firstRoute      *RouteInfoRecord
	deleteCalled    bool
	deleteCalledNum int
	deleteBefore    proto.RouteVersion
}

func (m *routeStorageMock) GetFirstRoute() (*RouteInfoRecord, error) {
	return m.firstRoute, nil
}

func (m *routeStorageMock) ListRoute() ([]*RouteInfoRecord, error) {
	return nil, nil
}

func (m *routeStorageMock) DeleteOldRoutes(before proto.RouteVersion) error {
	m.deleteCalled = true
	m.deleteCalledNum++
	m.deleteBefore = before
	return nil
}

func TestRouteItemRing(t *testing.T) {
	ring := newRouteItemRing(3)
	items, isLatest := ring.getFrom(3)
	assert.Equal(t, 0, len(items))
	assert.Equal(t, true, isLatest)

	for i := 1; i <= 3; i++ {
		item := &RouteItem{
			RouteVersion: proto.RouteVersion(i),
		}
		ring.put(item)
	}
	assert.Equal(t, proto.RouteVersion(1), ring.getMinVer())
	assert.Equal(t, proto.RouteVersion(3), ring.getMaxVer())

	items, isLatest = ring.getFrom(1)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, false, isLatest)

	items, isLatest = ring.getFrom(3)
	assert.Equal(t, 0, len(items))
	assert.Equal(t, true, isLatest)

	item4 := &RouteItem{
		RouteVersion: proto.RouteVersion(4),
	}
	ring.put(item4)
	assert.Equal(t, ring.getMinVer(), proto.RouteVersion(2))
	assert.Equal(t, ring.getMaxVer(), proto.RouteVersion(4))
}

func TestRemoveOldRouteItems_NoDeleteWhenStableLessThanTruncate(t *testing.T) {
	storage := &routeStorageMock{
		firstRoute: &RouteInfoRecord{RouteVersion: proto.RouteVersion(1)},
	}
	routeMgr := NewRouteMgr(10, false, nil, storage)
	routeMgr.stableRouteVersion = proto.RouteVersion(5)

	err := routeMgr.removeOldRouteItems(context.Background())
	assert.NoError(t, err)
	assert.False(t, storage.deleteCalled)
	assert.Equal(t, 0, storage.deleteCalledNum)
}
