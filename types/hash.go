package types

import "golang.org/x/crypto/blake2s"

func (h *Header) Hash() []byte {
	// TODO ideally i want to memoize hash, but it requires wrapping proto-generated structs
	digest, err := blake2s.New256(nil)
	if err != nil {
		panic(err.Error())
	}
	bytes, err := h.Marshal()
	if err != nil {
		panic(err.Error())
	}
	digest.Write(bytes)
	return digest.Sum(make([]byte, 0, digest.Size()))
}
