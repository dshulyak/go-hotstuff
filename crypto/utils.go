package crypto

import (
	"crypto/rand"
	"io"

	"github.com/kilic/bls12-381/blssig"
)

type (
	PublicKey  = blssig.PublicKey
	PrivateKey = blssig.SecretKey
)

func GenerateKey(rng io.Reader) (PrivateKey, *PublicKey, error) {
	if rng == nil {
		rng = rand.Reader
	}
	priv, err := blssig.RandSecretKey(rng)
	if err != nil {
		return nil, nil, err
	}
	return priv, blssig.PublicKeyFromSecretKey(priv), nil
}

func GenerateKeys(rng io.Reader, n int) ([]PublicKey, []PrivateKey, error) {
	pubs := make([]blssig.PublicKey, n)
	privs := make([]blssig.SecretKey, n)
	for i := range privs {
		priv, pub, err := GenerateKey(rng)
		if err != nil {
			return nil, nil, err
		}
		privs[i] = priv
		pubs[i] = *pub
	}
	return pubs, privs, nil
}
