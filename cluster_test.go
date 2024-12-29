package raft

import (
	"reflect"
	"testing"
)

func TestBuildCluster(t *testing.T) {

	tests := []struct {
		size   int
		indexs []int
	}{
		{1, nil},
	}

	for i, tt := range tests {
		_, nodes := buildCluster(tt.size, tt.indexs)

		base := ltoa(nodes[0].sm.log)
		for j, n := range nodes {
			l := ltoa(n.sm.log)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d.%d: log diff:\n%s", i, j, g)
			}

			// ensure same leader
			w := 0
			if tt.indexs != nil {
				w = tt.indexs[0]
			}

			if g := n.sm.lead; g != w {
				t.Errorf("#%d.%d: lead = %d, want %d", i, j, g, w)
			}

			p := map[int]struct{}{}
			for k := range n.sm.indexs {
				p[k] = struct{}{}
			}
			wp := map[int]struct{}{}
			for k := 0; k < tt.size; k++ {
				if tt.indexs != nil {
					wp[tt.indexs[k]] = struct{}{}
				} else {
					wp[k] = struct{}{}
				}
			}
			if !reflect.DeepEqual(p, wp) {
				t.Errorf("#%d.%d: peers = %+v, want %+v", i, j, p, wp)
			}
		}
	}
}

// TestBasicCluster ensures all nodes can send proposal to the cluster.
// And all the proposals will get committed.
func TestBasicCluster(t *testing.T) {
	tests := []struct {
		size  int
		round int
	}{
		{1, 3},
		{3, 3},
		{5, 3},
		{7, 3},
		{13, 1},
	}
	for i, tt := range tests {
		nt, nodes := buildCluster(tt.size, nil)
		for j := 0; j < tt.round; j++ {
			for _, n := range nodes {
				data := []byte{byte(n.Id())}
				nt.send(Message{Type: msgProp, To: n.Id(), Entries: []Entry{{Data: data}}})
				base := nodes[0].Next()
				if len(base) != 1 {
					t.Fatalf("#%d: len(ents) = %d, want 1", i, len(base))
				}
				if !reflect.DeepEqual(base[0].Data, data) {
					t.Errorf("#%d: data = %s, want %s", i, base[0].Data, data)
				}
				for k := 1; k < tt.size; k++ {
					g := nodes[k].Next()
					if !reflect.DeepEqual(g, base) {
						t.Errorf("#%d.%d: ent = %v, want %v", i, k, g, base)
					}
				}
			}
		}
	}
}

func buildCluster(size int, indexs []int) (nt *network, nodes []*Node) {
	if indexs == nil {
		indexs = make([]int, size)
		for i := 0; i < size; i++ {
			indexs[i] = i
		}
	}

	nodes = make([]*Node, size)
	nis := make([]Interface, size)
	for i := range nodes {
		nodes[i] = New(indexs[i], defaultHeartbeat, defaultElection)
		nis[i] = nodes[i]
	}
	nt = newNetwork(nis...)

	lead := dictate(nodes[0])
	lead.Next()
	for i := 1; i < size; i++ {
		lead.Add(indexs[i], "")
		nt.send(lead.Msgs()...)
		for j := 0; j < i; j++ {
			nodes[j].Next()
		}
	}

	for i := 0; i < 10*defaultHeartbeat; i++ {
		nodes[0].Tick()
	}
	msgs := nodes[0].Msgs()
	nt.send(msgs...)

	for _, n := range nodes {
		n.Next()
	}

	return
}
