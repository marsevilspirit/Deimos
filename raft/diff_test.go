package raft

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

func diffu(a, b string) string {
	if a == b {
		return ""
	}
	aname, bname := mustTemp("base", a), mustTemp("other", b)
	defer func() { _ = os.Remove(aname) }()
	defer func() { _ = os.Remove(bname) }()
	cmd := exec.Command("diff", "-u", aname, bname)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// do nothing
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

// return like /tmp/test657460961
func mustTemp(pre, body string) string {
	f, err := os.CreateTemp("", pre)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	_ = f.Close()

	fmt.Println(f.Name())

	return f.Name()
}

func ltoa(l *raftLog) string {
	s := fmt.Sprintf("committed: %d\n", l.committed)
	s += fmt.Sprintf("applied:  %d\n", l.applied)
	for i, e := range l.ents {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}
