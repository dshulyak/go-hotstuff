package hotstuff

import (
	"github.com/dshulyak/go-hotstuff/types"
	"github.com/syndtr/goleveldb/leveldb"
)

type Tag []byte

const (
	headerBucket = 1
	dataBucket   = 2
	certBucket   = 3

	viewBucket  = 4 // bucket to keep track of the last view
	votedBucket = 5 // bucket to keep track of the last voted view
)

var (
	PrepareTag = Tag("p")
	LockedTag  = Tag("l")
	DecideTag  = Tag("d")
	ExecTag    = Tag("e")
)

type BlockStore struct {
	db *leveldb.DB
}

func (s *BlockStore) GetHeader(hash []byte) (*types.Header, error) {
	return nil, nil
}

func (s *BlockStore) SaveHeader(header *types.Header) error {
	return nil
}

func (s *BlockStore) GetCertificate(hash []byte) (*types.Certificate, error) {
	return nil, nil
}

func (s *BlockStore) SaveCertificate(cert *types.Certificate) error {
	return nil
}

func (s *BlockStore) SaveData(hash []byte, data *types.Data) error {
	return nil
}

func (s *BlockStore) GetData(hash []byte) (*types.Data, error) {
	return nil, nil
}

func (s *BlockStore) SetTag(tag Tag, hash []byte) error {
	return nil
}

func (s *BlockStore) GetTagHeader(tag Tag) (*types.Header, error) {
	return nil, nil
}

func (s *BlockStore) GetTagCert(tag Tag) (*types.Certificate, error) {
	return nil, nil
}

func (s *BlockStore) GetTagData(tag Tag) (*types.Data, error) {
	return nil, nil
}

func (s *BlockStore) SaveView(view uint64) error {
	return nil
}

func (s *BlockStore) GetView() (uint64, error) {
	return 0, nil
}

func (s *BlockStore) SaveVoted(voted uint64) error {
	return nil
}

func (s *BlockStore) GetVoted() (uint64, error) {
	return 0, nil
}
