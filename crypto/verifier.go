package crypto

import (
	"crypto/ed25519"

	"github.com/dshulyak/go-hotstuff/types"
)

/*
Placeholder for BLS signatures.
*/

func NewEd25519Verifier(threshold int, pubkeys map[uint64]ed25519.PublicKey) *Ed25519Verifier {
	return &Ed25519Verifier{
		threshold: threshold,
		pubkeys:   pubkeys,
	}
}

type Ed25519Verifier struct {
	threshold int
	pubkeys   map[uint64]ed25519.PublicKey
}

func (v *Ed25519Verifier) VerifySingle(idx uint64, msg, sig []byte) bool {
	pub, exist := v.pubkeys[idx]
	if !exist {
		return false
	}
	return ed25519.Verify(pub, msg, sig)
}

func (v *Ed25519Verifier) VerifyCert(cert *types.Certificate) bool {
	count := 0
	for i, idx := range cert.Voters {
		valid := v.VerifySingle(idx, cert.Block, cert.Sigs[i])
		if !valid {
			return false
		}
		count++
	}
	return count >= v.threshold
}

func (v *Ed25519Verifier) Merge(cert *types.Certificate, vote *types.Vote) {
	cert.Voters = append(cert.Voters, vote.Voter)
	cert.Sigs = append(cert.Sigs, vote.Sig)
}
