package rtconf

import "github.com/sdeoras/kv"

type RtConf interface {
	kv.KV
	Update(key string, val []byte) error
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
