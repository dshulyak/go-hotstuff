package crypto

import "crypto/ed25519"

func GenerateKeys(n int) (map[uint64]ed25519.PublicKey, map[uint64]ed25519.PrivateKey) {
	privs := map[uint64]ed25519.PrivateKey{}
	pubs := map[uint64]ed25519.PublicKey{}
	for i := 0; i < n; i++ {
		pub, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			panic(err.Error())
		}
		idx := uint64(i)
		privs[idx] = priv
		pubs[idx] = pub
	}
	return pubs, privs
}
