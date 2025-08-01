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

package rpc2

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/stretchr/testify/require"
)

func TestConnection(t *testing.T) {
	{
		var conf ConnectorConfig
		conf.Network = "xxx"
		require.Panics(t, func() { defaultConnector(conf) })
	}
	{
		var conf ConnectorConfig
		conf.Network = "rdma"
		c := defaultConnector(conf)
		_, err := c.Get(testCtx, "")
		require.Error(t, err)
	}
}

type closedDialer struct {
	d Dialer
}

func (cd closedDialer) Dial(ctx context.Context, addr string) (transport.Conn, error) {
	conn, _ := cd.d.Dial(ctx, addr)
	conn.Close()
	return conn, nil
}

func TestConnectorOpenStream(t *testing.T) {
	addr, cli, shutdown := newTcpServer()
	defer shutdown()

	conf := cli.ConnectorConfig
	tc := *conf.Transport
	conf.Transport = &tc

	conf.Transport.MaxFrameSize += 1 << 30
	c := defaultConnector(conf)
	_, err := c.Get(testCtx, addr)
	require.Error(t, err)

	conf.Transport.MaxFrameSize -= 1 << 30
	c = defaultConnector(conf)
	_, err = c.Get(testCtx, addr)
	require.NoError(t, err)
	cc := c.(*connector)
	require.Equal(t, 1, len(cc.sessions[addr]))

	var sess *transport.Session
	for sess = range cc.sessions[addr] {
		sess.Close()
		break
	}
	_, in := cc.sessions[addr][sess]
	require.True(t, in)
	_, err = c.Get(testCtx, addr)
	require.NoError(t, err)
	_, in = cc.sessions[addr][sess]
	require.False(t, in)

	conf.Dialer = closedDialer{cc.dialer}
	c = defaultConnector(conf)
	_, err = c.Get(testCtx, addr)
	require.Error(t, err)
}

func TestConnectorConcurrent(t *testing.T) {
	addr, cli, shutdown := newTcpServer()
	defer shutdown()
	c := defaultConnector(cli.ConnectorConfig)

	var wg sync.WaitGroup
	wg.Add(3)
	for range [3]struct{}{} {
		go func() {
			for range [100]struct{}{} {
				stream, err := c.Get(testCtx, addr)
				if err != nil {
					panic(err)
				}
				c.Put(testCtx, stream, false)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestConnectorLimited(t *testing.T) {
	addr, cli, shutdown := newTcpServer()
	defer shutdown()
	c := defaultConnector(cli.ConnectorConfig).(*connector)
	t.Logf("%+v\n", c.Stats())

	for ii := 1; ii < c.config.MaxSessionPerAddress*c.config.MaxStreamPerSession; ii++ {
		_, err := c.Get(testCtx, addr)
		require.NoError(t, err)
	}
	t.Logf("%+v\n", c.Stats())
	stream, err := c.Get(testCtx, addr)
	require.NoError(t, err)
	_, err = c.Get(testCtx, addr)
	require.ErrorIs(t, ErrConnLimited, err)

	c.Put(testCtx, stream, false)
	stream1, err := c.Get(testCtx, addr)
	require.NoError(t, err)
	require.Equal(t, stream, stream1)

	c.Put(testCtx, stream, true)
	stream2, err := c.Get(testCtx, addr)
	require.NoError(t, err)
	require.NotEqual(t, stream, stream2)

	for ii := 0; ii < c.config.MaxStreamPerSession; ii++ {
		require.NoError(t, c.Put(testCtx, stream2, false))
	}
	t.Logf("%+v\n", c.Stats())
	require.NoError(t, c.Put(testCtx, stream2, false))
	require.True(t, stream2.IsClosed())
}

func TestConnectorNewSession(t *testing.T) {
	addr, cli, shutdown := newTcpServer()
	defer shutdown()
	cc := cli.ConnectorConfig
	cc.MaxSessionPerAddress = 1
	cc.MaxStreamPerSession = 1024
	c := defaultConnector(cc).(*connector)

	var hasErr atomic.Value
	hasErr.Store(false)
	var wg sync.WaitGroup
	wg.Add(cc.MaxStreamPerSession)
	for ii := 0; ii < cc.MaxStreamPerSession; ii++ {
		go func() {
			if _, err := c.Get(testCtx, addr); err != nil {
				hasErr.Store(true)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	require.False(t, hasErr.Load().(bool))
}
