package hotstuff

import "encoding/binary"

func EncodeUint64(u uint64) []byte {
	rst := make([]byte, 8)
	binary.BigEndian.PutUint64(rst, u)
	return rst
}

func DecodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
