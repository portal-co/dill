package util

import (
	"encoding/json"
	"io"

	shell "github.com/ipfs/go-ipfs-api"
)

type IpfsLazy[T any] struct {
	Src   string
	value *T
}

func GetLazy[T any](x IpfsLazy[T], sh *shell.Shell) *T {
	if x.value != nil {
		return x.value
	}
	r, err := sh.Cat(x.Src)
	if err != nil {
		return nil
	}
	d := json.NewDecoder(r)
	var y T
	d.Decode(&y)
	x.value = &y
	return x.value
}

func NewLazy[T any](x T, sh *shell.Shell) *IpfsLazy[T] {
	r, w := io.Pipe()
	e := json.NewEncoder(w)
	e.Encode(x)
	c, err := sh.Add(r)
	if err != nil {
		return nil
	}
	return &IpfsLazy[T]{Src: c}
}
