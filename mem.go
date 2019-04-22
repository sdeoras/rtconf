package rtconf

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type memdb struct {
	linksMutex    sync.Mutex
	watchersMutex sync.Mutex
	nameSpace     string
	links         map[string]*node
	watchers      map[string]*watchContexts
}

type watchContexts struct {
	ctxMap []context.CancelFunc
}

type node struct {
	value []byte
	links map[string]*node
}

// newMemRtConf provides a new instance of RtConf
func newMemRtConf() *memdb {
	m := new(memdb)
	m.nameSpace = "default"
	m.links = make(map[string]*node)
	n := new(node)
	n.links = make(map[string]*node)
	m.links[m.nameSpace] = n
	m.watchers = make(map[string]*watchContexts)
	return m
}

func (m *memdb) Get(key string) ([]byte, error) {
	n, ok := m.links[m.nameSpace]
	if !ok {
		return nil, fmt.Errorf("namespace not found")
	}

	for _, key := range splitKey(key, m.nameSpace) {
		if n.links == nil {
			return nil, fmt.Errorf("invalid key, key not found")
		}

		n, ok = n.links[key]
		if !ok {
			return nil, fmt.Errorf("invalid key, key not found")
		}
	}

	if len(n.links) != 0 || n.value == nil {
		return nil, fmt.Errorf("invalid key, key does not refer to leaf node:%d, %d",
			len(n.links), len(n.value))
	}

	b := make([]byte, len(n.value))
	for i := range n.value {
		b[i] = n.value[i]
	}

	return b, nil
}

func (m *memdb) Set(key string, val []byte) error {
	m.linksMutex.Lock()
	defer m.linksMutex.Unlock()

	if len(key) == 0 {
		return fmt.Errorf("cannot set empty key")
	}

	if val == nil {
		return fmt.Errorf("cannot set nil value, use zero value please")
	}

	n, ok := m.links[m.nameSpace]
	if !ok {
		return fmt.Errorf("namespace not found")
	}

	newNodeCreated := false
	for _, key := range splitKey(key, m.nameSpace) {
		if n.links == nil {
			n.links = make(map[string]*node)
		}

		if m, ok := n.links[key]; !ok {
			newNodeCreated = true
			m = new(node)
			m.links = make(map[string]*node)
			n.links[key] = m
			n = m
		} else {
			newNodeCreated = false
			n = m
		}
	}

	if len(n.links) != 0 {
		return fmt.Errorf("invalid key, key already exists and points to a bucket, not a value")
	}

	if !newNodeCreated {
		return fmt.Errorf("key already exists")
	}

	b := make([]byte, len(val))
	for i := range val {
		b[i] = val[i]
	}
	n.value = b

	return nil
}

func (m *memdb) Update(key string, val []byte) error {
	m.linksMutex.Lock()
	defer m.linksMutex.Unlock()

	if len(key) == 0 {
		return fmt.Errorf("cannot set empty key")
	}

	if val == nil {
		return fmt.Errorf("cannot set nil value, use zero value please")
	}

	n, ok := m.links[m.nameSpace]
	if !ok {
		return fmt.Errorf("namespace not found")
	}

	newNodeCreated := false
	for _, key := range splitKey(key, m.nameSpace) {
		if n.links == nil {
			n.links = make(map[string]*node)
		}

		if m, ok := n.links[key]; !ok {
			newNodeCreated = true
			m = new(node)
			m.links = make(map[string]*node)
			n.links[key] = m
			n = m
		} else {
			newNodeCreated = false
			n = m
		}
	}

	if len(n.links) != 0 {
		return fmt.Errorf("invalid key, key already exists and points to a bucket, not a value")
	}

	if newNodeCreated {
		if err := m.Delete(key); err != nil {
			return fmt.Errorf("key does not exist and also error deleting tmp data:%v", err)
		}
		return fmt.Errorf("key does not exists")
	}

	b := make([]byte, len(val))
	for i := range val {
		b[i] = val[i]
	}
	n.value = b

	m.watchersMutex.Lock()
	{
		if watcher, ok := m.watchers[filepath.Join(splitKey(key, m.nameSpace)...)]; ok {
			for _, cancel := range watcher.ctxMap {
				cancel()
			}
		}
		delete(m.watchers, filepath.Join(splitKey(key, m.nameSpace)...))
	}
	m.watchersMutex.Unlock()

	return nil
}

func (m *memdb) Delete(key string) error {
	m.linksMutex.Lock()
	defer m.linksMutex.Unlock()

	n, ok := m.links[m.nameSpace]
	if !ok {
		return fmt.Errorf("namespace not found")
	}

	parent := n
	keyToDelete := key

	for _, key := range splitKey(key, m.nameSpace) {
		if n.links == nil {
			return fmt.Errorf("invalid key, key not found")
		}

		parent = n
		keyToDelete = key

		n, ok = n.links[key]
		if !ok {
			return fmt.Errorf("invalid key, key not found")
		}
	}

	delete(parent.links, keyToDelete)

	return nil
}

func (m *memdb) Enumerate(key string) ([]string, error) {
	n, ok := m.links[m.nameSpace]
	if !ok {
		return nil, fmt.Errorf("namespace not found")
	}

	for _, key := range splitKey(key, m.nameSpace) {
		if n.links == nil {
			return nil, fmt.Errorf("invalid key, key not found")
		}

		n, ok = n.links[key]
		if !ok {
			return nil, fmt.Errorf("invalid key, key not found")
		}
	}

	keys := make([]string, 0, len(n.links))
	for k, v := range n.links {
		if len(v.links) > 0 {
			subKeys, err := m.Enumerate(filepath.Join(key, k))
			if err != nil {
				return nil, err
			}
			keys = append(keys, subKeys...)
		} else {
			if v.value != nil {
				keys = append(keys, filepath.Join(key, k))
			}
		}
	}

	return keys, nil
}

// Watch watches for changes in value for a key
func (m *memdb) Watch(key string) error {
	// implementation consists of creating parent context that can be cancelled
	// and allowing runtime config manager to cancel on updates.
	// Such context is derived from a timeout so we know it will always
	// eventually be done.
	ctx, tCancel := context.WithTimeout(context.Background(), time.Minute)
	defer tCancel()
	ctx, cancel := context.WithCancel(ctx)

	m.watchersMutex.Lock()
	{
		key = filepath.Join(splitKey(key, m.nameSpace)...)
		if watcher, ok := m.watchers[key]; !ok {
			watcher := new(watchContexts)
			m.watchers[key] = watcher
			watcher.ctxMap = make([]context.CancelFunc, 0, 0)
			watcher.ctxMap = append(watcher.ctxMap, cancel)
		} else {
			watcher.ctxMap = append(watcher.ctxMap, cancel)
		}
	}
	m.watchersMutex.Unlock()

	select {
	case <-ctx.Done():
	}

	return nil
}

func splitKey(key, nameSpace string) []string {
	key = filepath.Join(nameSpace, key)
	keys := strings.Split(key, "/")
	keys = keys[1:]
	return keys
}
