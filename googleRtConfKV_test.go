package rtconf

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

var (
	val = "val"
)

func TestGoogleRtConfKv_GetSet(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key)

	// get that thing
	if retVal, err := kv.Get(key); err != nil {
		t.Fatal(err)
	} else if string(retVal) != val {
		t.Fatal("not val")
	}
}

func TestGoogleRtConfKv_GetSetWrongKey(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key)

	// get that thing
	if val, err := kv.Get("wrongKey"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestGoogleRtConfKv_GetSetWrongBucket(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key)

	// get that thing
	if val, err := kv.Get("/a/b/d/this"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestGoogleRtConfKv_SetEmptyKey(t *testing.T) {
	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set("", []byte(val)); err == nil {
		t.Fatal("expected err here")
	}
}

func TestGoogleRtConfKv_GetEmptyKey(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key)

	// get that thing
	if val, err := kv.Get(""); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestGoogleRtConfKv_GetBucket(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key)

	// get that thing
	if val, err := kv.Get("/a/b/c"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestGoogleRtConfKv_SetNilValue(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, nil); err == nil {
		t.Fatal("expected err when trying to set a nil value")
	}
}

func TestGoogleRtConfKv_SetZeroValue(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte{}); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key)
}

func TestGoogleRtConfKv_GetZeroValue(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte{}); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key)

	if val, err := kv.Get(key); err != nil {
		t.Fatal(err)
	} else {
		if val == nil {
			t.Fatal("expected val to be zero length but not nil, got nil")
		} else {
			if len(val) != 0 {
				t.Fatal("expected val to be zero length, got:", len(val))
			}
		}
	}
}

func TestGoogleRtConfKv_DeleteKey(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// now delete the tree
	if err := kv.Delete(key); err != nil {
		t.Fatal(err)
	}

	// now ensure you can't get that thing
	if val, err := kv.Get(key); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got slice length:", len(val))
	}
}

func TestGoogleRtConfKv_DeleteTree(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := kv.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	// now delete the tree
	if err := kv.Delete("/a/b"); err != nil {
		t.Fatal(err)
	}

	// now ensure you can't get that thing
	if val, err := kv.Get(key); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got slice length:", len(val))
	}

	// now ensure you can't get that other thing as well
	if val, err := kv.Get(filepath.Join(bktName, "someOtherKey")); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got slice length:", len(val))
	}
}

func TestGoogleRtConfKv_DeleteDeletedKey(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	key2 := filepath.Join(bktName, "someOtherKey")
	// set something else in the same bucket
	if err := kv.Set(key2, []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key2)

	// now delete the key
	if err := kv.Delete(key); err != nil {
		t.Fatal(err)
	}

	// now delete the key
	if err := kv.Delete(key); err == nil {
		t.Fatal("expected error when deleting key twice")
	}
}

func TestGoogleRtConfKv_Enumerate(t *testing.T) {
	id := uuid.New().String()
	key := filepath.Join("a/b/c/", id)

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key)

	bktName, _ := filepath.Split(key)
	key2 := filepath.Join(bktName, "someOtherKey")
	// set something else in the same bucket
	if err := kv.Set(key2, []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key2)

	keys, err := kv.Enumerate("/a/b")
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range keys {
		switch _, v := filepath.Split(key); v {
		case id, "someOtherKey":
		default:
			t.Fatal("did not expect this key to be present in the list:", key)
		}
	}
}

func TestGoogleRtConfKv_DeleteEnumerate(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	key2 := filepath.Join(bktName, "someOtherKey")
	// set something else in the same bucket
	if err := kv.Set(key2, []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}
	defer kv.Delete(key2)

	if err := kv.Delete(key); err != nil {
		t.Fatal(err)
	}

	keys, err := kv.Enumerate("/a/b/")
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 1 {
		t.Fatal("expected only one key, found:", len(keys))
	}

	for _, key := range keys {
		switch _, v := filepath.Split(key); v {
		case "someOtherKey":
		default:
			t.Fatal("did not expect this key to be present in the list:", key)
		}
	}
}

func TestGoogleRtConfKv_DeleteAllEnumerate(t *testing.T) {
	key := filepath.Join("a/b/c/", uuid.New().String())

	kv, err := NewGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	key2 := filepath.Join(bktName, "someOtherKey")
	// set something else in the same bucket
	if err := kv.Set(key2, []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	if err := kv.Delete(key); err != nil {
		t.Fatal(err)
	}

	if err := kv.Delete(key2); err != nil {
		t.Fatal(err)
	}

	keys, err := kv.Enumerate("/a/b/")
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) > 0 {
		fmt.Println(keys)
		t.Fatal("did not expect any key to be listed, found:", len(keys))
	}
}
