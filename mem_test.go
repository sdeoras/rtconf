package rtconf

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

var (
	key = "a/b/c/myKey"
)

func TestMemRt_Watch(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(time.Second)
		_ = rt.Update(key, []byte{1, 2, 3})
	}()

	// get that thing
	if err := rt.Watch(key); err != nil {
		t.Fatal(err)
	}
}

func TestMemRt_GetSet(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if retVal, err := rt.Get(key); err != nil {
		t.Fatal(err)
	} else if string(retVal) != val {
		t.Fatal("not val")
	}
}

func TestMemRt_GetSetWrongKey(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := rt.Get("wrongKey"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestMemRt_GetSetWrongBucket(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := rt.Get("/a/b/d/this"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestMemRt_SetEmptyKey(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set("", []byte(val)); err == nil {
		t.Fatal("expected err here")
	}
}

func TestMemRt_GetEmptyKey(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := rt.Get(""); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestMemRt_GetBucket(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := rt.Get("/a/b/c"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestMemRt_SetNilValue(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, nil); err == nil {
		t.Fatal("expected err when trying to set a nil value")
	}
}

func TestMemRt_SetZeroValue(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte{}); err != nil {
		t.Fatal(err)
	}
}

func TestMemRt_GetZeroValue(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte{}); err != nil {
		t.Fatal(err)
	}

	if val, err := rt.Get(key); err != nil {
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

func TestMemRt_DeleteKey(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// now delete the tree
	if err := rt.Delete(key); err != nil {
		t.Fatal(err)
	}

	// now ensure you can't get that thing
	if val, err := rt.Get(key); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got slice length:", len(val))
	}
}

func TestMemRt_DeleteTree(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := rt.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	// now delete the tree
	if err := rt.Delete("/a/b"); err != nil {
		t.Fatal(err)
	}

	// now ensure you can't get that thing
	if val, err := rt.Get(key); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got slice length:", len(val))
	}

	// now ensure you can't get that other thing as well
	if val, err := rt.Get(filepath.Join(bktName, "someOtherKey")); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got slice length:", len(val))
	}
}

func TestMemRt_DeleteDeletedKey(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := rt.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	// now delete the key
	if err := rt.Delete(key); err != nil {
		t.Fatal(err)
	}

	// now delete the key
	if err := rt.Delete(key); err == nil {
		t.Fatal("expected error when deleting key twice")
	}
}

func TestMemRt_Enumerate(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := rt.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	keys, err := rt.Enumerate("/a/b/")
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range keys {
		switch _, v := filepath.Split(key); v {
		case "myKey", "someOtherKey":
		default:
			t.Fatal("did not expect this key to be present in the list:", key)
		}
	}
}

func TestMemRt_DeleteEnumerate(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := rt.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	if err := rt.Delete(key); err != nil {
		t.Fatal(err)
	}

	keys, err := rt.Enumerate("/a/b/")
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

func TestMemRt_DeleteAllEnumerate(t *testing.T) {
	rt := NewMemRtConf()

	// set something
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := rt.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	if err := rt.Delete(key); err != nil {
		t.Fatal(err)
	}

	if err := rt.Delete(filepath.Join(bktName, "someOtherKey")); err != nil {
		t.Fatal(err)
	}

	keys, err := rt.Enumerate("/a/b/")
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) > 0 {
		fmt.Println(keys)
		t.Fatal("did not expect any key to be listed, found:", len(keys))
	}
}
