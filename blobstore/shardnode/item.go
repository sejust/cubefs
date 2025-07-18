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

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
)

func (s *service) insertItem(ctx context.Context, req *shardnode.InsertItemArgs) error {
	if len(req.Item.ID) < 1 {
		return apierr.ErrItemIDEmpty
	}
	if len(req.Item.ID) > storage.MaxKeySize {
		return apierr.ErrKeySizeTooLarge
	}
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return err
	}
	return space.InsertItem(ctx, req.GetHeader(), req.GetItem())
}

func (s *service) updateItem(ctx context.Context, req *shardnode.UpdateItemArgs) error {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return err
	}
	return space.UpdateItem(ctx, req.GetHeader(), req.GetItem())
}

func (s *service) deleteItem(ctx context.Context, req *shardnode.DeleteItemArgs) error {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return err
	}
	return space.DeleteItem(ctx, req.GetHeader(), req.GetID())
}

func (s *service) getItem(ctx context.Context, req *shardnode.GetItemArgs) (item shardnode.Item, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return
	}
	item, err = space.GetItem(ctx, req.GetHeader(), req.GetID())
	return
}

func (s *service) listItem(ctx context.Context, req *shardnode.ListItemArgs) (resp shardnode.ListItemRet, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return
	}
	items, nextMarker, err := space.ListItem(ctx, req.GetHeader(), req.GetPrefix(), req.GetMarker(), req.GetCount())
	if err != nil {
		return
	}
	resp.Items = items
	resp.NextMarker = nextMarker
	return
}
