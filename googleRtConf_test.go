package rtconf

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestGoogleRuntimeConfig_Delete(t *testing.T) {
	rt, err := newGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	key := uuid.New().String()
	val := "some unique value"
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(1, err)
	}

	if err := rt.Delete(key); err != nil {
		t.Fatal(err)
	}
}
func TestGoogleRuntimeConfig_SetGet(t *testing.T) {
	rt, err := newGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	key := uuid.New().String()
	val := "some unique value"
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(1, err)
	}
	defer rt.Delete(key)

	retVal, err := rt.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if string(retVal) != val {
		t.Fatal("expected:", val, "got:", string(retVal))
	}
}

func TestGoogleRuntimeConfig_SetGetUpdate(t *testing.T) {
	rt, err := newGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	key := uuid.New().String()
	val := "some unique value"
	val2 := "other updated value"
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(1, err)
	}
	defer rt.Delete(key)

	retVal, err := rt.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if string(retVal) != val {
		t.Fatal("expected:", val, "got:", string(retVal))
	}

	if err := rt.Update(key, []byte(val2)); err != nil {
		t.Fatal(err)
	}

	retVal, err = rt.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if string(retVal) != val2 {
		t.Fatal("expected:", val2, "got:", string(retVal))
	}
}

func TestGoogleRuntimeConfig_Enumerate(t *testing.T) {
	rt, err := newGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "my-config")
	if err != nil {
		t.Fatal(err)
	}

	key := uuid.New().String()
	val := "some unique value"
	if err := rt.Set(key, []byte(val)); err != nil {
		t.Fatal(1, err)
	}

	defer rt.Delete(key)

	keys, err := rt.Enumerate(key)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 1 {
		t.Fatal("expected 1 key to be returned, got:", len(keys))
	}

	if filepath.Base(keys[0]) != key {
		t.Fatal("expected:", key, "got:", filepath.Base(keys[0]))
	}
}
