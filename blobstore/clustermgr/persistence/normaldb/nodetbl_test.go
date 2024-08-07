package normaldb

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var nr1 = NodeInfoRecord{
	Version:   NodeInfoVersionNormal,
	NodeID:    proto.NodeID(1),
	ClusterID: proto.ClusterID(1),
	Idc:       "z0",
	Rack:      "rack1",
	Host:      "127.0.0.1",
	Status:    proto.NodeStatusNormal,
	Role:      proto.NodeRoleBlobNode,
	DiskType:  proto.DiskTypeHDD,
}

var nr2 = NodeInfoRecord{
	Version:   NodeInfoVersionNormal,
	NodeID:    proto.NodeID(2),
	ClusterID: proto.ClusterID(1),
	Idc:       "z0",
	Rack:      "rack2",
	Host:      "127.0.0.2",
	Status:    proto.NodeStatusNormal,
	Role:      proto.NodeRoleBlobNode,
	DiskType:  proto.DiskTypeHDD,
}

func TestNodeTbl(t *testing.T) {
	tmpDBPath := os.TempDir() + "/" + uuid.NewString() + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	nodeTbl, err := OpenNodeTable(db)
	require.NoError(t, err)

	// get all node/ add node / delete node
	nodeList, err := nodeTbl.GetAllNodes()
	require.NoError(t, err)
	require.Equal(t, 0, len(nodeList))

	err = nodeTbl.UpdateNode(&nr1)
	require.NoError(t, err)

	err = nodeTbl.UpdateNode(&nr2)
	require.NoError(t, err)

	nodeList, err = nodeTbl.GetAllNodes()
	require.NoError(t, err)
	require.Equal(t, 2, len(nodeList))
}
