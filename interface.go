package rtconf

import "github.com/sdeoras/kv"

type RtConf interface {
	kv.KV
	Update(key string, val []byte) error
}
