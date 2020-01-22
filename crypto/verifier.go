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

func (v *Ed25519Verifier) VerifyCert(msg []byte, sig *types.AggregatedSignature) bool {
	count := 0
	for i, idx := range sig.Voters {
		valid := v.VerifySingle(idx, msg, sig.Sigs[i])
		if !valid {
			return false
		}
		count++
	}
	return count >= v.threshold
}

func (v *Ed25519Verifier) Merge(asig *types.AggregatedSignature, voter uint64, sig []byte) {
	asig.Voters = append(asig.Voters, voter)
	asig.Sigs = append(asig.Sigs, sig)
}
