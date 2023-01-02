package action

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/exec"
	"sort"
	"sync"

	shell "github.com/ipfs/go-ipfs-api"
	"github.com/portal-co/dill/util"
	"go.starlark.net/starlark"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

type Action struct {
	Cmd  []string
	Deps map[string]string
}

func (a Action) Run(me string, sh *shell.Shell) (string, error) {
	rd := []string{}
	var e errgroup.Group
	var em sync.Mutex
	for k, v := range a.Deps {
		k := k
		v := v
		e.Go(func() error {
			il := util.IpfsLazy[Action]{Src: fmt.Sprintf("%s/%s", me, v)}
			g := util.GetLazy(il, sh)
			r, err := g.Run(il.Src, sh)
			if err != nil {
				return err
			}
			em.Lock()
			defer em.Unlock()
			rd = append(rd, fmt.Sprintf("%s:%s", r, k))
			return nil
		})
	}
	err := e.Wait()
	if err != nil {
		return "", err
	}
	sort.Strings(rd)
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

func RunAction(me string, sh *shell.Shell) (string, error) {
	a := util.GetLazy(util.IpfsLazy[Action]{Src: me + "/action"}, sh)
	return a.Run(me, sh)
}

func hash_fnv(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

type ActionBuild struct {
	Cmd     []string
	AbsDeps map[string]ActionBuildDep
}

type ActionBuildDep struct {
	Act  ActionBuild
	Name string
}

func (d ActionBuildDep) Attr(name string) (starlark.Value, error) {
	if name == "action" {
		return d.Act, nil
	}
	return starlark.String(d.Name), nil
}

func (d ActionBuildDep) AttrNames() []string {
	return []string{"action", "name"}
}

func (b ActionBuild) AttrNames() []string {
	return append([]string{"_cmd", "_built"}, maps.Keys(b.AbsDeps)...)
}

func (d ActionBuild) Attr(name string) (starlark.Value, error) {
	e, ok := d.AbsDeps[name]
	if ok {
		return e, nil
	}
	if name == "_cmd" {
		r := make([]starlark.Value, len(d.Cmd))
		for i, s := range d.Cmd {
			r[i] = starlark.String(s)
		}
		return starlark.NewList(r), nil
	}
	if name == "_built" {
		return starlark.NewBuiltin("built", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var sh ActionCtx
			starlark.UnpackArgs("built", args, kwargs, "shell", &sh)
			t, err := CreateAction(d, sh.sh)
			if err != nil {
				return nil, err
			}
			r, err := RunAction(t, sh.sh)
			if err != nil {
				return nil, err
			}
			return starlark.String(r), nil
		}), nil
	}
	return nil, nil
}

func (c ActionCtx) AttrNames() []string {
	return []string{"dep", "act", "cat", "add", "add_dir", "import", "export"}
}

func (c ActionCtx) Attr(name string) (starlark.Value, error) {
	return starlark.NewBuiltin(name, ctxAttrs[name](c.sh)), nil
}

var ctxAttrs map[string]func(*shell.Shell) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) = map[string]func(*shell.Shell) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error){
	"dep": func(s *shell.Shell) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		return func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var act ActionBuild
			var name string
			err := starlark.UnpackArgs("dep", args, kwargs, "act", &act, "name", &name)
			return ActionBuildDep{Act: act, Name: name}, err
		}
	},
	"act": func(s *shell.Shell) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		return func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var deps map[string]ActionBuildDep
			var cmd []string
			err := starlark.UnpackArgs("act", args, kwargs, "deps", &deps, "cmd", &cmd)
			return ActionBuild{AbsDeps: deps, Cmd: cmd}, err
		}
	},
	"cat": func(s *shell.Shell) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		return func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var cid string
			err := starlark.UnpackArgs("cat", args, kwargs, "cid", &cid)
			if err != nil {
				return nil, err
			}
			c, err := s.Cat(cid)
			if err != nil {
				return nil, err
			}
			defer c.Close()
			b, err := io.ReadAll(c)
			return starlark.Bytes(b), err
		}
	},
	"add": func(s *shell.Shell) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		return func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var content []byte
			err := starlark.UnpackArgs("add", args, kwargs, "content", &content)
			if err != nil {
				return nil, err
			}
			c, err := s.Add(bytes.NewBuffer(content))
			if err != nil {
				return nil, err
			}
			return starlark.String(c), nil
		}
	},
	"import": func(s *shell.Shell) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		return func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var content string
			err := starlark.UnpackArgs("import", args, kwargs, "content", &content)
			if err != nil {
				return nil, err
			}
			return ConsumeAction(content, s)
		}
	},
	"export": func(s *shell.Shell) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		return func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var content ActionBuild
			err := starlark.UnpackArgs("export", args, kwargs, "content", &content)
			if err != nil {
				return nil, err
			}
			t, err := CreateAction(content, s)
			return starlark.String(t), err
		}
	},
	"add_dir": func(s *shell.Shell) func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		return func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var content map[string]string
			err := starlark.UnpackArgs("add_dir", args, kwargs, "content", &content)
			if err != nil {
				return nil, err
			}
			d, err := util.AddDir(s, content)
			if err != nil {
				return nil, err
			}
			return starlark.String(d), nil
		}
	},
}

type ActionCtx struct {
	sh *shell.Shell
}

func (s ActionCtx) String() string {
	return "ctx"
}
func (d ActionBuildDep) String() string {
	return d.Name + " => " + d.Act.String()
}

func (b ActionBuild) String() string {
	r := fmt.Sprint(b.Cmd)
	for i, o := range b.AbsDeps {
		r = fmt.Sprintf("%s\n[%s <= %s]", r, i, o.String())
	}
	return r
}

func (a ActionCtx) Type() string {
	return "ctx"
}

func (d ActionBuildDep) Type() string {
	return "dep"
}

func (d ActionBuild) Type() string {
	return "build"
}

func (d ActionBuildDep) Freeze() {

}

func (d ActionBuild) Freeze() {

}

func (d ActionCtx) Freeze() {

}

// Truth returns the truth value of an object.
func (d ActionBuildDep) Truth() starlark.Bool {
	return starlark.Bool(true)
}

func (d ActionBuild) Truth() starlark.Bool {
	return starlark.Bool(true)
}

func (d ActionCtx) Truth() starlark.Bool {
	return starlark.Bool(true)
}

// Hash returns a function of x such that Equals(x, y) => Hash(x) == Hash(y).
// Hash may fail if the value's type is not hashable, or if the value
// contains a non-hashable value. The hash is used only by dictionaries and
// is not exposed to the Starlark program.
func (d ActionBuildDep) Hash() (uint32, error) {
	return hash_fnv(d.String()), nil
}

func (d ActionBuild) Hash() (uint32, error) {
	return hash_fnv(d.String()), nil
}

func (d ActionCtx) Hash() (uint32, error) {
	return hash_fnv(d.String()), fmt.Errorf("cannot hash a context")
}

func CreateAction(act ActionBuild, sh *shell.Shell) (string, error) {
	m := map[string]string{}
	a := Action{Cmd: act.Cmd, Deps: map[string]string{}}
	for k, v := range act.AbsDeps {
		w, err := CreateAction(v.Act, sh)
		if err != nil {
			return "", err
		}
		m[k] = w
		a.Deps[v.Name] = k
	}
	r := util.NewLazy(a, sh)
	m["action"] = r.Src
	return util.AddDir(sh, m)
}

func ConsumeAction(path string, sh *shell.Shell) (ActionBuild, error) {
	var b ActionBuild
	a := util.GetLazy(util.IpfsLazy[Action]{Src: path + "/action"}, sh)
	b.Cmd = a.Cmd
	for k, v := range a.Deps {
		w, err := ConsumeAction(path+"/"+v, sh)
		if err != nil {
			return b, err
		}
		b.AbsDeps[k] = ActionBuildDep{
			Name: k,
			Act:  w,
		}
	}
	return b, nil
}
