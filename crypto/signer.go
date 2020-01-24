package crypto

import (
	"github.com/kilic/bls12-381/blssig"
)

/*
TODO this signature doesn't have any protection against rogue key attack
*/

var (
	// TODO separate domains for timeout and regular certificate
	domain = [8]byte{1, 1, 1}
)

func NewBLS12381Signer(priv blssig.SecretKey) *BLS12381Signer {
	return &BLS12381Signer{
		priv: priv,
	}
}

type BLS12381Signer struct {
	priv blssig.SecretKey
}

func (s *BLS12381Signer) Sign(dst, msg []byte) []byte {
	if len(msg) != 32 {
		panic("message must be exactly 32 bytes")
	}
	m := [32]byte{}
	copy(m[:], msg)
	sig := blssig.Sign(m, domain, s.priv)
	// this conversion is quit inneficient, and should be done on the caller side when signature will be sent over
	// the wiree
	return append(dst, blssig.SignatureToCompressed(sig)...)
}
