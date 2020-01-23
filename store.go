package hotstuff

import (
	"github.com/dshulyak/go-hotstuff/types"
	"github.com/syndtr/goleveldb/leveldb"
)

type Tag []byte

const (
	headerBucket byte = iota + 1
	dataBucket
	certBucket
	viewBucket  // bucket to keep track of the last view
	votedBucket // bucket to keep track of the last voted view
)

var (
	PrepareTag = Tag("p")
	LockedTag  = Tag("l")
	DecideTag  = Tag("d")
	ExecTag    = Tag("e")
)

func partsKey(part byte, hash []byte) []byte {
	key := make([]byte, len(hash)+1)
	key[0] = part
	copy(key[1:], hash)
	return key
}

func headerKey(hash []byte) []byte {
	return partsKey(headerBucket, hash)
}

func dataKey(hash []byte) []byte {
	return partsKey(dataBucket, hash)
}

func certKey(hash []byte) []byte {
	return partsKey(certBucket, hash)
}

func NewBlockStore(db *leveldb.DB) *BlockStore {
	return &BlockStore{db: db}
}

type BlockStore struct {
	db *leveldb.DB

	// TODO majority of this writes need to be fsynced
	// also all writes for the same event should be batched
	// therefore use batch for all writes and write it with Sync=true
}

func (s *BlockStore) GetHeader(hash []byte) (*types.Header, error) {
	header := &types.Header{}
	buf, err := s.db.Get(headerKey(hash), nil)
	if err != nil {
		return nil, err
	}
	if err := header.Unmarshal(buf); err != nil {
		return nil, err
	}
	return header, nil
}

func (s *BlockStore) SaveHeader(header *types.Header) error {
	buf, err := header.Marshal()
	if err != nil {
		return err
	}
	return s.db.Put(headerKey(header.Hash()), buf, nil)
}

func (s *BlockStore) GetCertificate(hash []byte) (*types.Certificate, error) {
	cert := &types.Certificate{}
	buf, err := s.db.Get(certKey(hash), nil)
	if err != nil {
		return nil, err
	}
	if err := cert.Unmarshal(buf); err != nil {
		return nil, err
	}
	return cert, nil
}

func (s *BlockStore) SaveCertificate(cert *types.Certificate) error {
	buf, err := cert.Marshal()
	if err != nil {
		return err
	}
	return s.db.Put(certKey(cert.Block), buf, nil)
}

func (s *BlockStore) SaveData(hash []byte, data *types.Data) error {
	buf, err := data.Marshal()
	if err != nil {
		return err
	}
	return s.db.Put(dataKey(hash), buf, nil)
}

func (s *BlockStore) GetData(hash []byte) (*types.Data, error) {
	data := &types.Data{}
	buf, err := s.db.Get(dataKey(hash), nil)
	if err != nil {
		return nil, err
	}
	if err := data.Unmarshal(buf); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *BlockStore) SetTag(tag Tag, hash []byte) error {
	return s.db.Put([]byte(tag), hash, nil)
}

func (s *BlockStore) GetTag(tag Tag) ([]byte, error) {
	return s.db.Get([]byte(tag), nil)
}

func (s *BlockStore) GetTagHeader(tag Tag) (*types.Header, error) {
	hash, err := s.GetTag(tag)
	if err != nil {
		return nil, err
	}
	return s.GetHeader(hash)
}

func (s *BlockStore) GetTagCert(tag Tag) (*types.Certificate, error) {
	hash, err := s.GetTag(tag)
	if err != nil {
		return nil, err
	}
	return s.GetCertificate(hash)
}

func (s *BlockStore) SaveView(view uint64) error {
	return s.db.Put([]byte{viewBucket}, EncodeUint64(view), nil)
}

func (s *BlockStore) GetView() (uint64, error) {
	data, err := s.db.Get([]byte{viewBucket}, nil)
	if err != nil {
		return 0, err
	}
	return DecodeUint64(data), nil
}

func (s *BlockStore) SaveVoted(voted uint64) error {
	return s.db.Put([]byte{votedBucket}, EncodeUint64(voted), nil)
}

func (s *BlockStore) GetVoted() (uint64, error) {
	data, err := s.db.Get([]byte{votedBucket}, nil)
	if err != nil {
		return 0, err
	}
	return DecodeUint64(data), nil
}

func (s *BlockStore) SaveBlock(block *types.Block) error {
	if err := s.SaveHeader(block.Header); err != nil {
		return err
	}
	if err := s.SaveCertificate(block.Cert); err != nil {
		return err
	}
	if err := s.SaveData(block.Header.Hash(), block.Data); err != nil {
		return err
	}
	return nil
}

func (s *BlockStore) GetBlock(hash []byte) (*types.Block, error) {
	header, err := s.GetHeader(hash)
	if err != nil {
		return nil, err
	}
	cert, err := s.GetCertificate(hash)
	if err != nil {
		return nil, err
	}
	data, err := s.GetData(hash)
	if err != nil {
		return nil, err
	}
	return &types.Block{Header: header, Cert: cert, Data: data}, nil
}

// TODO add iteration from older to newer using secondary index
// key <chain>|parent|child and scan by <chain>|parent|*

func NewChainIterator(store *BlockStore) *ChainIterator {
	return &ChainIterator{
		store: store,
	}
}

func NewChainIteratorFromLatest(store *BlockStore) (*ChainIterator, error) {
	header, err := store.GetTagHeader(PrepareTag)
	if err != nil {
		return nil, err
	}
	return NewChainIteratorFrom(store, header), nil
}

func NewChainIteratorFrom(store *BlockStore, from *types.Header) *ChainIterator {
	return &ChainIterator{
		store:  store,
		header: from,
	}
}

type ChainIterator struct {
	store *BlockStore
	err   error

	header      *types.Header
	certificate *types.Certificate
	data        *types.Data
}

func (ci *ChainIterator) Err() error {
	return ci.err
}

func (ci *ChainIterator) Valid() bool {
	return ci.err == nil
}

func (ci *ChainIterator) Next() {
	ci.certificate = nil
	ci.data = nil
	if ci.header == nil {
		ci.header, ci.err = ci.store.GetTagHeader(DecideTag)
	} else {
		ci.header, ci.err = ci.store.GetHeader(ci.header.Parent)
	}
}

func (ci *ChainIterator) Ceritificate() *types.Certificate {
	if ci.certificate == nil {
		ci.certificate, ci.err = ci.store.GetCertificate(ci.header.Hash())
	}
	return ci.certificate
}

func (ci *ChainIterator) Data() *types.Data {
	if ci.data == nil {
		ci.data, ci.err = ci.store.GetData(ci.header.Hash())
	}
	return ci.data
}

func (ci *ChainIterator) Header() *types.Header {
	return ci.header
}
