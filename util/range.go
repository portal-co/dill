package util

import (
	"math/rand"

	"golang.org/x/exp/constraints"
)

func Range[T constraints.Integer](f, e T) []T {
	x := []T{}
	for y := f; y < e+1; y++ {
		x = append(x, y)
	}
	return x
}

func Shuffle[T any](s *[]T, r *rand.Rand) {
	slice := *s
	for i := range slice {
		j := r.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
	*s = slice
}

func ShuffleSeed[T any](s *[]T, seed int64) {
	Shuffle(s, rand.New(rand.NewSource(seed)))
}
