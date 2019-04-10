package rtconf

import (
	"fmt"
	"os"
	"testing"
)

func TestGoogleRuntimeConfig_Get(t *testing.T) {
	rt := newGoogleRtConf(os.Getenv("GOOGLE_PROJECT"), "test")
	val, err := rt.Get("x")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(string(val))
}
