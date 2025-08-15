package store

import (
	"bytes"
	"compress/gzip"
	"io"
	"path"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/marsevilspirit/deimos/store/storepb"
)

// buildSnapshotPB builds a protobuf snapshot
func buildSnapshotPB(s *store) *storepb.StoreSnapshot {
	return &storepb.StoreSnapshot{
		BaseIndex:    0,
		CurrentIndex: s.CurrentIndex,
		Metadata: storepb.StoreMetadata{
			Version:           int32(s.CurrentVersion),
			TotalTransactions: s.Stats.TotalTranscations(),
			WatcherCount:      uint64(s.WatcherHub.count),
		},
		RootNode: *nodeToPB(s.Root),
	}
}

// buildSnapshotPBCompressed builds a gzip-compressed protobuf snapshot
func buildSnapshotPBCompressed(s *store) ([]byte, error) {
	snapshot := buildSnapshotPB(s)

	// First serialize to protobuf
	pbData, err := proto.Marshal(snapshot)
	if err != nil {
		return nil, err
	}

	// Directly compress protobuf data with gzip
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)

	if _, err := gw.Write(pbData); err != nil {
		return nil, err
	}

	if err := gw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// applySnapshotPB applies a protobuf snapshot to store
func applySnapshotPB(s *store, snap *storepb.StoreSnapshot) {
	s.CurrentIndex = snap.CurrentIndex
	s.CurrentVersion = int(snap.Metadata.Version)

	// Rebuild Root
	s.Root = pbToNode(s, &snap.RootNode, nil)

	// Rebuild watcher/stats/ttl heap
	s.WatcherHub = newWatchHub(1000)
	s.Stats = newStats()
	s.ttlKeyHeap = newTtlKeyHeap()
	s.Root.recoverAndclean()
}

// applySnapshotPBCompressed restores store from a gzip-compressed protobuf snapshot
func applySnapshotPBCompressed(s *store, compressedData []byte) error {
	// First decompress gzip
	gr, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return err
	}
	defer gr.Close()

	// Read decompressed protobuf data
	pbData, err := io.ReadAll(gr)
	if err != nil {
		return err
	}

	// Deserialize protobuf
	var snap storepb.StoreSnapshot
	if err := proto.Unmarshal(pbData, &snap); err != nil {
		return err
	}

	// Apply snapshot
	applySnapshotPB(s, &snap)
	return nil
}

func nodeToPB(n *node) *storepb.NodeData {
	pb := &storepb.NodeData{
		Path:          n.Path,
		CreatedIndex:  n.CreatedIndex,
		ModifiedIndex: n.ModifiedIndex,
		Acl:           n.ACL,
		IsDir:         n.IsDir(),
	}
	if !n.ExpireTime.IsZero() {
		v := n.ExpireTime.Unix()
		pb.ExpireUnix = &v
	}
	if n.IsDir() {
		// Directory: sort by key for stable order, then recursively write
		children := make([]*node, 0, len(n.Children))
		for _, ch := range n.Children {
			children = append(children, ch)
		}
		sort.Slice(children, func(i, j int) bool { return children[i].Path < children[j].Path })
		for _, ch := range children {
			pb.Children = append(pb.Children, *nodeToPB(ch))
		}
	} else {
		val, _ := n.Read()
		pb.Value = &val
	}
	return pb
}

func pbToNode(s *store, pb *storepb.NodeData, parent *node) *node {
	var n *node
	expire := Permanent
	if pb.ExpireUnix != nil {
		expire = time.Unix(*pb.ExpireUnix, 0)
	}
	if pb.IsDir {
		n = newDir(s, pb.Path, pb.CreatedIndex, parent, pb.Acl, expire)
		// children
		for i := range pb.Children {
			child := pbToNode(s, &pb.Children[i], n)
			// Extract child name from path
			_, name := path.Split(child.Path)
			n.Children[name] = child
		}
	} else {
		val := ""
		if pb.Value != nil {
			val = *pb.Value
		}
		n = newKV(s, pb.Path, val, pb.CreatedIndex, parent, pb.Acl, expire)
	}
	n.ModifiedIndex = pb.ModifiedIndex
	return n
}
