package rtconf

import (
	"context"
	"encoding/base64"
	"path/filepath"
	"strings"

	runtimeconfig "google.golang.org/api/runtimeconfig/v1beta1"
)

const (
	projects  = "projects"
	configs   = "configs"
	variables = "variables"
)

type googleRuntimeConfig struct {
	service         *runtimeconfig.Service
	projectsService *runtimeconfig.ProjectsService
	nameSpace       string
	projectId       string
}

func newGoogleRtConf(projectId, nameSpace string) (*googleRuntimeConfig, error) {
	g := new(googleRuntimeConfig)
	g.nameSpace = nameSpace
	g.projectId = projectId

	s, err := runtimeconfig.NewService(context.Background())
	if err != nil {
		return nil, err
	}

	ps := runtimeconfig.NewProjectsService(s)
	g.projectsService = ps

	g.service = s

	return g, nil
}

func (g *googleRuntimeConfig) Get(key string) ([]byte, error) {
	key = filepath.Join(g.nameSpace, key)
	keys := strings.Split(key, "/")
	key = filepath.Join(keys[1:]...)

	myKey := filepath.Join(projects, g.projectId, configs, g.nameSpace, variables, key)
	rt, err := g.projectsService.Configs.Variables.Get(myKey).Do()
	if err != nil {
		return nil, err
	}

	return base64.StdEncoding.DecodeString(rt.Value)
}

func (g *googleRuntimeConfig) Set(key string, val []byte) error {
	key = filepath.Join(g.nameSpace, key)
	keys := strings.Split(key, "/")
	key = filepath.Join(keys[1:]...)

	parent := filepath.Join(projects, g.projectId, configs, g.nameSpace)
	variable := new(runtimeconfig.Variable)
	variable.Name = filepath.Join(parent, variables, key)
	variable.Value = base64.StdEncoding.EncodeToString(val)

	_, err := g.projectsService.Configs.Variables.Create(parent, variable).Do()
	return err
}

func (g *googleRuntimeConfig) Delete(key string) error {
	key = filepath.Join(g.nameSpace, key)
	keys := strings.Split(key, "/")
	key = filepath.Join(keys[1:]...)

	key = filepath.Join(projects, g.projectId, configs, g.nameSpace, variables, key)

	_, err := g.projectsService.Configs.Variables.Delete(key).Do()
	return err
}

func (g *googleRuntimeConfig) Enumerate(key string) ([]string, error) {
	key = filepath.Join(g.nameSpace, key)
	keys := strings.Split(key, "/")
	key = filepath.Join(keys[1:]...)

	parent := filepath.Join(projects, g.projectId, configs, g.nameSpace)

	listCall := g.projectsService.Configs.Variables.List(parent).
		Filter(filepath.Join(parent, variables, key)).
		ReturnValues(false).
		PageSize(10)

	resp, err := listCall.Do()
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(resp.Variables))
	for _, variable := range resp.Variables {
		out = append(out, variable.Name)
	}

	return out, nil
}

func (g *googleRuntimeConfig) Update(key string, val []byte) error {
	key = filepath.Join(g.nameSpace, key)
	keys := strings.Split(key, "/")
	key = filepath.Join(keys[1:]...)

	key = filepath.Join(projects, g.projectId, configs, g.nameSpace, variables, key)
	variable := new(runtimeconfig.Variable)
	variable.Name = key
	variable.Value = base64.StdEncoding.EncodeToString(val)

	_, err := g.projectsService.Configs.Variables.Update(key, variable).Do()
	return err
}
