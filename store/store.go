package store

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"time"
)

// global store
var store *Store

const (
	ERROR = -1 + iota
	SET
	DELETE
	GET
)

var PERMANENT = time.Unix(0, 0)

type Store struct {
	// use the build-in hash map as the key-value store structure
	// Nodes map[string]Node `json:"nodes"`

	// use treeMap as the key-value store structure
	Tree *tree
	// the string channel to send messages to the outside world
	// now we use it to send changes to the hub of the web service
	messager *chan string

	ResponseMap map[string]Response

	ResponseMaxSize int

	ResponseCurrSize uint

	// at some point, we may need to compact the Response
	ResponseStartIndex uint64

	// current Index
	Index uint64
}

type Node struct {
	Value string `json:"value"`

	// if the node is a permanent one the ExpireTime will be Unix(0,0)
	// Otherwise after the expireTime, the node will be deleted
	ExpireTime time.Time `json:"expireTime"`

	// a channel to update the expireTime of the node
	update chan time.Time `json:"-"`
}

type Response struct {
	Action    int    `json:"action"`
	Key       string `json:"key"`
	PrevValue string `json:"prevValue"`
	Value     string `json:"value"`

	// if the key existed before the action, this field should be true
	// if did not exist before the action, should be flase
	Exist bool `json:"exist"`

	Expiration time.Time `json:"expiration"`

	// countdown until expiration in seconds
	TTL int64 `json:"TTL"`

	Index uint64 `json:"index"`
}

type ListNode struct {
	Key   string
	Value string
	Type  string
}

// make a new store
func createStore() *Store {
	return &Store{
		messager:           nil,
		ResponseMap:        make(map[string]Response),
		ResponseStartIndex: 0,
		ResponseMaxSize:    1024,
		ResponseCurrSize:   0,
		Tree: &tree{
			&treeNode{
				Value: Node{
					Value:      "/",
					ExpireTime: PERMANENT,
					update:     nil,
				},
				Dir:     true,
				NodeMap: make(map[string]*treeNode),
			},
		},
	}
}

// return a pointer to the global store
func GetStore() *Store {
	return store
}

// set the messager of the store
func (s *Store) SetMessager(messager *chan string) {
	s.messager = messager
}

// set the key to value, return the old value if the key exists
func Set(key string, value string, expireTime time.Time, index uint64) ([]byte, error) {
	// update index
	store.Index = index

	key = "/" + key

	key = path.Clean(key)

	var isExpire bool = false

	isExpire = !expireTime.Equal(PERMANENT)

	// when the slow follower receive the set command
	// the key may be expired, we should not add the node
	// also if the node exist, we need to delete the node
	if isExpire && expireTime.Sub(time.Now()) < 0 {
		return Delete(key, index)
	}

	var TTL int64
	// update ttl
	if isExpire {
		TTL = int64(expireTime.Sub(time.Now()) / time.Second)
	} else {
		TTL = -1
	}

	// get the node
	node, ok := store.Tree.get(key)

	if ok {
		// if node is not permanent before
		// update its expireTime
		if !node.ExpireTime.Equal(PERMANENT) {
			node.update <- expireTime
		} else {
			// if we want the permanent node to have expire time
			// we need to create a chan and create a go routine
			if isExpire {
				node.update = make(chan time.Time)
				go expire(key, node.update, expireTime)
			}
		}

		// update the infomation of the node
		store.Tree.set(key, Node{value, expireTime, node.update})

		resp := Response{
			Action:     SET,
			Key:        key,
			PrevValue:  node.Value,
			Value:      value,
			Exist:      true,
			Expiration: expireTime,
			TTL:        TTL,
			Index:      index,
		}

		msg, err := json.Marshal(resp)

		notify(resp)

		// send to the messager
		if store.messager != nil && err == nil {
			*store.messager <- string(msg)
		}

		updateMap(index, &resp)

		return msg, err
	} else { // add new node
		update := make(chan time.Time)

		store.Tree.set(key, Node{value, expireTime, update})

		if isExpire {
			go expire(key, update, expireTime)
		}

		resp := Response{
			Action:     SET,
			Key:        key,
			PrevValue:  "",
			Value:      value,
			Exist:      false,
			Expiration: expireTime,
			TTL:        TTL,
			Index:      index,
		}

		msg, err := json.Marshal(resp)

		// nofity the watcher
		notify(resp)

		// notify the web interface
		if store.messager != nil && err == nil {
			*store.messager <- string(msg)
		}

		updateMap(index, &resp)
		fmt.Println(index - store.ResponseStartIndex)
		return msg, err
	}
}

// get the value of key
func Get(key string) Response {
	key = "/" + key
	key = path.Clean(key)
	node, ok := store.Tree.get(key)
	if ok {
		var TTL int64
		var isExpire bool = false

		isExpire = !node.ExpireTime.Equal(PERMANENT)

		// update ttl
		if isExpire {
			TTL = int64(node.ExpireTime.Sub(time.Now()) / time.Second)
		} else {
			TTL = -1
		}

		resp := Response{
			Action:     GET,
			Key:        key,
			PrevValue:  node.Value,
			Value:      node.Value,
			Exist:      true,
			Expiration: node.ExpireTime,
			TTL:        TTL,
			Index:      store.Index,
		}
		return resp
	} else {
		resp := Response{
			Action:     GET,
			Key:        key,
			PrevValue:  "",
			Value:      "",
			Exist:      false,
			Expiration: PERMANENT,
			TTL:        0,
			Index:      store.Index,
		}
		return resp
	}
}

// List all the item in the prefix
func List(prefix string) ([]byte, error) {
	var ln []ListNode
	nodes, keys, dirs, ok := store.Tree.list(prefix)
	if ok {
		ln = make([]ListNode, len(nodes))
		for i := range nodes {
			ln[i] = ListNode{
				Key:   keys[i],
				Value: nodes[i].Value,
				Type:  dirs[i],
			}
		}
	}
	return json.Marshal(ln)
}

// delete the key
func Delete(key string, index uint64) ([]byte, error) {
	// update index
	store.Index = index
	key = "/" + key
	key = path.Clean(key)
	node, ok := store.Tree.get(key)
	if ok {
		store.Tree.delete(key)

		if node.ExpireTime.Equal(PERMANENT) {
			store.Tree.delete(key)
		} else {
			// kill the expire go routine
			node.update <- PERMANENT
			store.Tree.delete(key)
		}

		resp := Response{
			Action:     DELETE,
			Key:        key,
			PrevValue:  node.Value,
			Value:      "",
			Exist:      true,
			Expiration: node.ExpireTime,
			TTL:        0,
			Index:      index,
		}

		msg, err := json.Marshal(resp)

		notify(resp)

		// notify the messager
		if (store.messager != nil) && (err == nil) {
			*store.messager <- string(msg)
		}
		updateMap(index, &resp)
		return msg, err
	} else {
		resp := Response{
			Action:     DELETE,
			Key:        key,
			PrevValue:  "",
			Value:      "",
			Exist:      false,
			Expiration: PERMANENT,
			TTL:        0,
			Index:      index,
		}
		updateMap(index, &resp)
		return json.Marshal(resp)
	}
}

// set the value of the key to the value if the given prevValue is equal to the value of the key
func TestAndSet(key string, preValue string, value string, expireTime time.Time, index uint64) ([]byte, error) {
	resp := Get(key)

	if resp.PrevValue == preValue {
		return Set(key, value, expireTime, index)
	} else {
		return json.Marshal(resp)
	}
}

// should be used as a go routine to delete the key when it expires
func expire(key string, update chan time.Time, expireTime time.Time) {
	duration := expireTime.Sub(time.Now())
	for {
		select {
		// timeout delete the node
		case <-time.After(duration):
			node, ok := store.Tree.get(key)
			if !ok {
				return
			} else {
				store.Tree.delete(key)

				resp := Response{
					Action:     DELETE,
					Key:        key,
					PrevValue:  node.Value,
					Value:      "",
					Exist:      true,
					Expiration: node.ExpireTime,
					TTL:        0,
					Index:      store.Index,
				}

				msg, err := json.Marshal(resp)

				notify(resp)

				// notify the messager
				if store.messager != nil && err == nil {
					*store.messager <- string(msg)
				}
				return
			}
		case updateTime := <-update:
			// update the duration
			// if the node become a permanent node,
			// the go routine is not needed
			if updateTime.Equal(PERMANENT) {
				fmt.Println("PERMANENT")
				return
			}
			// update the duration
			duration = updateTime.Sub(time.Now())
		}
	}
}

func updateMap(index uint64, resp *Response) {
	if store.ResponseMaxSize == 0 {
		return
	}

	strIndex := strconv.FormatUint(index, 10)
	store.ResponseMap[strIndex] = *resp

	// unlimited
	if store.ResponseMaxSize < 0 {
		store.ResponseCurrSize++
		return
	}

	if store.ResponseCurrSize == uint(store.ResponseMaxSize) {
		store.ResponseStartIndex++
		delete(store.ResponseMap, strconv.FormatUint(store.ResponseStartIndex, 10))
	} else {
		store.ResponseCurrSize++
	}
}

// save the current state of the storage system
// now use json to save the state temporarily
func (s *Store) Save() ([]byte, error) {
	b, err := json.Marshal(store)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return b, nil
}

func (s *Store) Recovery(state []byte) error {
	err := json.Unmarshal(state, store)
	// clean the expired nodes
	clean()
	return err
}

// clean all expired keys
func clean() {
	store.Tree.traverse(cleanNode, true)
}

func cleanNode(key string, node *Node) {
	if node.ExpireTime.Equal(PERMANENT) {
		return
	} else {
		if node.ExpireTime.Sub(time.Now()) >= time.Second {
			node.update = make(chan time.Time)
			go expire(key, node.update, node.ExpireTime)
		} else {
			// we should delete the node
			store.Tree.delete(key)
		}
	}
}
