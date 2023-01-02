package util

func MapMap[K comparable, A any, B any, F func(A) B](m map[K]A, fn F) map[K]B {
	if m == nil {
		return nil
	}
	n := map[K]B{}
	for k, v := range m {
		n[k] = fn(v)
	}
	return n
}

func MapMap2[K comparable, A any, B any, C comparable, F func(A) (B, C)](m map[K]A, fn F) (n map[K]B, c C) {
	if m == nil {
		n = nil
		return
	}
	n = map[K]B{}
	for k, v := range m {
		w, err := fn(v)
		var d C
		if err != d {
			n = nil
			c = err
			return
		}
		n[k] = w
	}
	return
}

func MapMap2E[K comparable, A any, B any, F func(A) (B, error)](m map[K]A, fn F) (n map[K]B, c error) {
	if m == nil {
		n = nil
		return
	}
	n = map[K]B{}
	for k, v := range m {
		w, err := fn(v)
		if err != nil {
			n = nil
			c = err
			return
		}
		n[k] = w
	}
	return
}
