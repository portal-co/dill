package util

import (
	"os"
	"sort"

	shell "github.com/ipfs/go-ipfs-api"
	"golang.org/x/exp/constraints"
	"golang.org/x/sync/errgroup"
)

func SortedKeys[K constraints.Ordered, V any](m map[K]V) []K {
	keys := make([]K, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func AddDir(sh *shell.Shell, d map[string]string) (string, error) {
	empty, err := sh.NewObject("unixfs-dir")
	if err != nil {
		return "", err
	}

	curdir := empty
	for _, k := range SortedKeys(d) {

		name := k
		nobj, err := sh.PatchLink(curdir, name, d[k], true)
		if err != nil {
			return "", err
		}
		curdir = nobj
	}

	return curdir, nil
}

func AddPath(sh *shell.Shell, path string, patch map[string]string) (string, error) {
	s, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if s.IsDir() {
		d, err := os.ReadDir(path)
		if err != nil {
			return "", err
		}
		var m map[string]string = map[string]string{}
		g := new(errgroup.Group)
		for _, e := range d {
			e := e
			g.Go(func() error {
				p := path + e.Name()
				if x, ok := patch[p]; ok {
					m[e.Name()] = x
					return nil
				}
				x, err := AddPath(sh, p, patch)
				if err != nil {
					return err
				}
				m[e.Name()] = x
				return nil
			})
		}
		err = g.Wait()
		if err != nil {
			return "", err
		}
		return AddDir(sh, m)
	} else {
		f, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer f.Close()
		return sh.Add(f)
	}
}

func Patch(sh *shell.Shell, path string, patch map[string]string) (string, error) {
	l, er := sh.List(path)
	if er == nil {
		var m map[string]string
		g := new(errgroup.Group)
		for _, e := range l {
			e := e
			g.Go(func() error {
				p := path + e.Name
				if x, ok := patch[p]; ok {
					m[e.Name] = x
					return nil
				}
				x, err := Patch(sh, p, patch)
				if err != nil {
					return err
				}
				m[e.Name] = x
				return nil
			})
		}
		err := g.Wait()
		if err != nil {
			return "", err
		}
		return AddDir(sh, m)
	} else {
		// file
		return sh.ResolvePath(path)
	}
}
