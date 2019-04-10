package rtconf

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

const (
	googleRtConfUrlFormatter = "https://runtimeconfig.googleapis.com/v1beta1/projects/%s/configs/%s/variables"
)

func getGoogleRtConfUrl(projectId, nameSpace string) string {
	return fmt.Sprintf(googleRtConfUrlFormatter, projectId, nameSpace)
}

type googleRuntimeConfig struct {
	nameSpace string
	url       string
}

func newGoogleRtConf(projectId, nameSpace string) RtConf {
	g := new(googleRuntimeConfig)
	g.nameSpace = nameSpace
	g.url = getGoogleRtConfUrl(projectId, nameSpace)
	return g
}

type GoogleRtConfResponse struct {
	Name       string    `json:"name"`
	Value      string    `json:"value"`
	UpdateTime time.Time `json:"updateTime"`
}

func (g *googleRuntimeConfig) Get(key string) ([]byte, error) {
	key = filepath.Join(g.nameSpace, key)
	keys := strings.Split(key, "/")
	key = filepath.Join(keys[1:]...)

	url := fmt.Sprintf("%s/%s", g.url, key)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s:%s. Mesg:%s", "expected status 200 OK, got", resp.Status, string(b))
	}

	r := new(GoogleRtConfResponse)
	if err := json.Unmarshal(b, r); err != nil {
		return nil, err
	}

	return base64.StdEncoding.DecodeString(r.Value)
}

func (g *googleRuntimeConfig) Set(key string, val []byte) error {
	return nil
}

func (g *googleRuntimeConfig) Delete(key string) error {
	return nil
}

func (g *googleRuntimeConfig) Enumerate(key string) ([]string, error) {
	return nil, nil
}

func (g *googleRuntimeConfig) Update(key string, val []byte) error {
	return nil
}
