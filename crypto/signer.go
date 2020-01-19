package crypto

import (
	"crypto/ed25519"
)

func NewEd25519Signer(priv ed25519.PrivateKey) *Ed25519Signer {
	return &Ed25519Signer{priv: priv}
}

type Ed25519Signer struct {
	priv ed25519.PrivateKey
}

// Sign appends signature to dst and returns it.
func (s *Ed25519Signer) Sign(dst, msg []byte) []byte {
	sig := ed25519.Sign(s.priv, msg)
	if dst == nil {
		return sig
	}
	return append(dst, sig...)
}
