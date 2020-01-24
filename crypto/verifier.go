package crypto

import (
	"github.com/dshulyak/go-hotstuff/types"
	"github.com/kilic/bls12-381/blssig"
)

func NewBLS12381Verifier(threshold int, pubkeys []blssig.PublicKey) *BLS12381Verifier {
	return &BLS12381Verifier{
		threshold: threshold,
		pubkeys:   pubkeys,
	}
}

type BLS12381Verifier struct {
	threshold int
	pubkeys   []blssig.PublicKey
}

func (v *BLS12381Verifier) Verify(idx uint64, msg, sig []byte) bool {
	if idx >= uint64(len(v.pubkeys)) {
		return false
	}
	key := v.pubkeys[idx]
	signature, err := blssig.NewSignatureFromCompresssed(sig)
	if err != nil {
		return false
	}
	m := [32]byte{}
	copy(m[:], msg)
	return blssig.Verify(m, domain, signature, &key)
}

func (v *BLS12381Verifier) VerifyAggregated(msg []byte, asig *types.AggregatedSignature) bool {
	if len(asig.Voters) < v.threshold {
		return false
	}
	pubs := make([]*blssig.PublicKey, 0, len(asig.Voters))
	for _, voter := range asig.Voters {
		if voter >= uint64(len(v.pubkeys)) {
			return false
		}
		pubs = append(pubs, &v.pubkeys[voter])
	}
	sig, err := blssig.NewSignatureFromCompresssed(asig.Sig)
	if err != nil {
		return false
	}
	m := [32]byte{}
	copy(m[:], msg)
	return blssig.VerifyAggregateCommon(m, domain, pubs, sig)
}

func (v *BLS12381Verifier) Merge(asig *types.AggregatedSignature, voter uint64, sig []byte) {
	if voter >= uint64(len(v.pubkeys)) {
		return
	}
	for _, v := range asig.Voters {
		if v == voter {
			return
		}
	}
	if asig.Sig != nil {
		sig1, err := blssig.NewSignatureFromCompresssed(asig.Sig)
		if err != nil {
			return
		}
		sig2, err := blssig.NewSignatureFromCompresssed(sig)
		if err != nil {
			return
		}
		sig1 = blssig.AggregateSignature(sig1, sig2)
		asig.Sig = blssig.SignatureToCompressed(sig1)
	} else {
		asig.Sig = sig
	}
	asig.Voters = append(asig.Voters, voter)
}
