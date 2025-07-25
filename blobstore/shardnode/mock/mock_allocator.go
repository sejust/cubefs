// Code generated by MockGen. DO NOT EDIT.
// Source: ../catalog/allocator/allocator.go

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	clustermgr "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	codemode "github.com/cubefs/cubefs/blobstore/common/codemode"
	proto "github.com/cubefs/cubefs/blobstore/common/proto"
	gomock "github.com/golang/mock/gomock"
)

// MockAllocator is a mock of Allocator interface.
type MockAllocator struct {
	ctrl     *gomock.Controller
	recorder *MockAllocatorMockRecorder
}

// MockAllocatorMockRecorder is the mock recorder for MockAllocator.
type MockAllocatorMockRecorder struct {
	mock *MockAllocator
}

// NewMockAllocator creates a new mock instance.
func NewMockAllocator(ctrl *gomock.Controller) *MockAllocator {
	mock := &MockAllocator{ctrl: ctrl}
	mock.recorder = &MockAllocatorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAllocator) EXPECT() *MockAllocatorMockRecorder {
	return m.recorder
}

// AllocSlices mocks base method.
func (m *MockAllocator) AllocSlices(ctx context.Context, codeMode codemode.CodeMode, fileSize uint64, sliceSize uint32, failedVids []proto.Vid) ([]proto.Slice, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllocSlices", ctx, codeMode, fileSize, sliceSize, failedVids)
	ret0, _ := ret[0].([]proto.Slice)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllocSlices indicates an expected call of AllocSlices.
func (mr *MockAllocatorMockRecorder) AllocSlices(ctx, codeMode, fileSize, sliceSize, failedVids interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllocSlices", reflect.TypeOf((*MockAllocator)(nil).AllocSlices), ctx, codeMode, fileSize, sliceSize, failedVids)
}

// Close mocks base method.
func (m *MockAllocator) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockAllocatorMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockAllocator)(nil).Close))
}

// ListVolume mocks base method.
func (m *MockAllocator) ListVolume(ctx context.Context, codeMode codemode.CodeMode) ([]clustermgr.AllocVolumeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListVolume", ctx, codeMode)
	ret0, _ := ret[0].([]clustermgr.AllocVolumeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListVolume indicates an expected call of ListVolume.
func (mr *MockAllocatorMockRecorder) ListVolume(ctx, codeMode interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListVolume", reflect.TypeOf((*MockAllocator)(nil).ListVolume), ctx, codeMode)
}
