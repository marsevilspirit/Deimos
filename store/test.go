package store

import (
	"math/rand"
	"strconv"
)

func GenKeys(num int, depth int) []string {
	keys := make([]string, num)
	for i := range num {
		keys[i] = "/foo/"
		depth := rand.Intn(depth) + 1

		for range depth {
			keys[i] += "/" + strconv.Itoa(rand.Int())
		}
	}
	return keys
}
