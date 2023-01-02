package action

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	shell "github.com/ipfs/go-ipfs-api"
	"github.com/portal-co/dill/util"
)

type Action struct {
	Cmd  []string
	Deps map[string]string
}

func (a Action) Run(me string, sh *shell.Shell) (string, error) {
	rd := []string{}
	for k, v := range a.Deps {
		il := util.IpfsLazy[Action]{Src: fmt.Sprintf("%s/%s", me, v)}
		g := util.GetLazy(il, sh)
		r, err := g.Run(il.Src, sh)
		if err != nil {
			return "", err
		}
		rd = append(rd, fmt.Sprintf("%s:%s", r, k))
	}
	aa := []string{os.Getenv("WRAPPER")}
	aa = append(aa, rd...)
	aa = append(aa, "--")
	aa = append(aa, a.Cmd...)
	p := exec.Command(aa[0], aa[1:]...)
	out, err := p.StdoutPipe()
	if err != nil {
		return "", err
	}
	defer out.Close()
	ec := make(chan error)
	go func() {
		ec <- p.Run()
	}()
	r, err := io.ReadAll(out)
	if err != nil {
		return "", err
	}
	err = <-ec
	if err != nil {
		return "", err
	}
	s := string(r)
	return s, nil
}

type ActionBuild struct {
	Cmd      []string
	AbsDeps  map[string]ActionBuild
	DepNames map[string]string
}

func CreateAction(act ActionBuild, sh *shell.Shell) (string, error) {
	m := map[string]string{}
	for k, v := range act.AbsDeps {
		w, err := CreateAction(v, sh)
		if err != nil {
			return "", err
		}
		m[k] = w
	}
	a := Action{Cmd: act.Cmd, Deps: map[string]string{}}
	for k := range act.AbsDeps {
		a.Deps[act.DepNames[k]] = k
	}
	r := util.NewLazy(a, sh)
	m["action"] = r.Src
	return util.AddDir(sh, m)
}
