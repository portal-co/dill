package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"

	shell "github.com/ipfs/go-ipfs-api"
	"github.com/portal-co/dill/util"
	"golang.org/x/crypto/ssh/terminal"
)

func main() {
	sh := shell.NewLocalShell()
	interactive := os.Args[1] == "-i"
	if interactive {
		os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
	}
	e := os.Environ()
	sort.Strings(e)
	// fmt.Printf("%s\n", e)
	var hhh string
	var in io.Reader
	in = os.Stdin
	ch := make(chan []byte)
	if !interactive {
		h := sha256.New()
		for _, f := range e {
			h.Write([]byte(f))
		}
		for _, b := range os.Args[1:] {
			h.Write([]byte(b))
		}
		if !terminal.IsTerminal(0) {
			y, err := io.ReadAll(in)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err.Error())
				return
			}
			h.Write(y)
			in = bytes.NewBuffer(y)
		}
		b := base64.URLEncoding.EncodeToString(h.Sum([]byte{}))
		c := os.Getenv("PRTL_CACHE")
		r, err := os.ReadFile(c)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err.Error())
			return
		}
		if string(r) != "" {
			l, err := sh.List(string(r))
			if err == nil {
				for _, m := range l {
					if m.Name == b {
						o, err := sh.List(m.Hash)
						if err == nil {
							for _, p := range o {
								if p.Name == "output" {
									c, err := sh.Cat(p.Hash)
									if err == nil {
										defer c.Close()
										io.Copy(os.Stderr, c)
									}
								} else if p.Name == "results" {
									fmt.Print(p.Hash)
								}
							}
							return
						}
					}
				}
			}
		}
		hhh = b
	}
	t, _ := os.MkdirTemp("/tmp", "")
	defer func() {
		h, err := util.AddPath(sh, t+"/", map[string]string{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err.Error())
			return
		}
		if !interactive {
			j := <-ch
			i := bytes.NewBuffer(j)
			a, err := sh.Add(i)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err.Error())
				return
			}
			os.Stderr.Write(j)
			fmt.Fprint(os.Stdout, h)
			h, err = util.AddDir(sh, map[string]string{"results": h, "output": a})
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err.Error())
				return
			}
		} else {
			os.Stderr.Write(<-ch)
			fmt.Fprint(os.Stdout, h)
		}
		if !interactive {
			b := hhh
			c := os.Getenv("PRTL_CACHE")
			g, err := os.OpenFile(c+".lock", os.O_EXCL|os.O_CREATE, 0777)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err.Error())
				return
			}
			defer g.Close()
			f, err := os.ReadFile(c)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err.Error())
				return
			}
			var l string
			if string(f) != "" {
				m, err := sh.PatchLink(string(f), b, h, true)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s", err.Error())
					return
				}
				l = m
			} else {
				m, err := util.AddDir(sh, map[string]string{b: h})
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s", err.Error())
					return
				}
				l = m
			}
			err = os.WriteFile(c, []byte(l), 0777)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err.Error())
				return
			}
		}
	}()
	a := os.Args[1:]
	c := []string{"bwrap", "--ro-bind", "/", "/", "--dev", "/dev", "--proc", "/proc", "--bind", t, t, "--chdir", t}
	for a[0] != "--" {
		s := strings.SplitN(a[0], ":", 2)
		c = append(c, "--ro-bind", "/ipfs/"+s[0], "/tmp/deps/"+s[1])
		a = a[1:]
	}
	a = a[1:]
	c = append(c, a...)
	m := exec.Command(c[0], c[1:]...)
	m.Stdin = in
	out, err := m.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err.Error())
		return
	}
	go func() {
		ch <- out
	}()
}
