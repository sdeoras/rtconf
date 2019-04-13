package rtconf

import "github.com/sdeoras/kv"

// RtConf defines a runtime config interface.
type RtConf interface {
	// KV defines Set, Get, Delete and Enumerate
	kv.KV

	// Update updates a variable value
	Update(key string, val []byte) error

	// Watch watches for key value changes at times newer
	// than when the watch was started
	Watch(key string) error
}

// NewGoogleRtConf provides a google runtime configurator backed implementation for
// RtConf interface
func NewGoogleRtConf(projectId, nameSpace string) (RtConf, error) {
	return newGoogleRtConf(projectId, nameSpace)
}

// NewGoogleRtKv provides a google runtime configurator backed implementation for
// kv.KV interface
func NewGoogleRtKv(projectId, nameSpace string) (kv.KV, error) {
	return newGoogleRtConf(projectId, nameSpace)
}
