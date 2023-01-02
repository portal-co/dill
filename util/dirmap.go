package util

import (
	"sync"

	shell "github.com/ipfs/go-ipfs-api"
)

type DirMap[V any] struct {
	Src   string
	Mutex sync.Mutex
}

func NewD[V any](m map[string]V, sh *shell.Shell) (*DirMap[V], error) {
	n := make(map[string]string)
	for a, b := range m {
		c := NewLazy(b, sh)
		n[a] = c.Src
	}
	o, err := AddDir(sh, n)
	if err != nil {
		return nil, err
	}
	return &DirMap[V]{Src: o}, nil
}

func RenderD[V any](v DirMap[V], sh *shell.Shell) (map[string]V, error) {
	k, err := KeysD(sh, v)
	if err != nil {
		return nil, err
	}
	r := map[string]V{}
	for _, l := range k {
		m := GetLazy(GetD(v, l), sh)
		r[l] = *m
	}
	return r, nil
}

func MapD[V any, W any, F func(V) (W, error)](sh *shell.Shell, d DirMap[V], fn F) (*DirMap[W], error) {
	r, err := RenderD(d, sh)
	if err != nil {
		return nil, err
	}
	m, err := MapMap2E(r, fn)
	if err != nil {
		return nil, err
	}
	return NewD(m, sh)
}

func GetD[V any](m DirMap[V], k string) IpfsLazy[V] {
	return IpfsLazy[V]{Src: m.Src + "/" + k}
}

func PutD[V any](sh *shell.Shell, m *DirMap[V], k string, v IpfsLazy[V]) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	x, err := sh.PatchLink(m.Src, k, v.Src, true)
	if err != nil {
		return err
	}
	m.Src = x
	return nil
}

func KeysD[V any](sh *shell.Shell, x DirMap[V]) ([]string, error) {
	l, err := sh.List(x.Src)
	if err != nil {
		return nil, err
	}
	m := make([]string, len(l))
	for i, n := range l {
		m[i] = n.Name
	}
	return m, nil
}
