package util

import (
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
)

type Me struct {
	Priv *rsa.PrivateKey
	Pub  *rsa.PublicKey
}

func Hash(x interface{}) []byte {
	h := sha256.New()
	g := gob.NewEncoder(h)
	g.Encode(x)
	return h.Sum([]byte{})
}

func CatchJar(j string, me Me) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(j)
	if err != nil {
		return nil, err
	}
	d, err := rsa.DecryptOAEP(sha256.New(), nil, me.Priv, b, append([]byte("portal-"), Hash(me.Pub)...))
	if err != nil {
		return nil, rsa.ErrVerification
	}
	return d, nil
}

func MkJar(tgt []byte, me Me) string {
	x, _ := rsa.EncryptOAEP(sha256.New(), nil, me.Pub, tgt, append([]byte("portal-"), Hash(me.Pub)...))
	return base64.StdEncoding.EncodeToString(x)
}
