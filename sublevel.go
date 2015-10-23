package sublevel

import (
	"github.com/jmhodges/levigo"
)

type Iterator interface {
	Close()
	GetError() error
	Key() []byte
	Next()
	Prev()
	Seek(key []byte)
	SeekToFirst()
	SeekToLast()
	Valid() bool
	Value() []byte
}

type Hook interface {
	Delete(key []byte, sublevel *DB)
	Put(key, value []byte, sublevel *DB)
}

type HookFunc func(key, value []byte, hook Hook)

type WriteBatch struct {
	db *DB
	batch *levigo.WriteBatch
	prefix []byte
}

type DB struct {
	db *levigo.DB
	prefix []byte
	hooks []HookFunc
}

type sublevelIterator struct {
	it *levigo.Iterator
	prefix []byte
	valid bool
}

type tmpHook struct {
	db *DB
	batch *levigo.WriteBatch
	key []byte
	value []byte
}

type tmpBatchHook struct {
	db *DB
	batch *levigo.WriteBatch
}

/**************************************************************
 *
 * sublevelDB
 *
 **************************************************************/

func Sublevel(db *levigo.DB, prefix string) *DB {
	return &DB{db: db, prefix: append([]byte(prefix),0)}
}

func (this *DB) LevelDB() *levigo.DB {
	return this.db
}

func (this *DB) AddHook(hook HookFunc) {
	this.hooks = append(this.hooks, hook)
}

func (this *DB) Delete(wo *levigo.WriteOptions, key []byte) error {
	if len(this.hooks) != 0 {
		tmp := &tmpHook{db: this, key: key, value: nil}
		for _, h := range this.hooks {
			h(key, nil, tmp)
		}
		if tmp.batch != nil {
			err := this.db.Write(wo, tmp.batch)
			tmp.batch.Close()
			return err
		}
	}
	newkey := append(this.prefix, key...)
	return this.db.Delete(wo, newkey)
}

func (this *DB) DeleteInBatch(key []byte, batch *levigo.WriteBatch) error {
	if len(this.hooks) != 0 {
		tmp := &tmpHook{db: this, key: key, value: nil, batch: batch}
		for _, h := range this.hooks {
			h(key, nil, tmp)
		}
	}
	newkey := append(this.prefix, key...)
	batch.Delete(newkey)
	return nil
}

func (this *DB) Get(ro *levigo.ReadOptions, key []byte) ([]byte, error) {
	newkey := append(this.prefix, key...)
	return this.db.Get(ro, newkey)
}

func (this *DB) NewIterator(ro *levigo.ReadOptions) Iterator {
	it := &sublevelIterator{it: this.db.NewIterator(ro), prefix: this.prefix}
	return it
}

func (this *DB) Put(wo *levigo.WriteOptions, key, value []byte) error {
	if len(this.hooks) != 0 {
		tmp := &tmpHook{db: this, key: key, value: value}
		for _, h := range this.hooks {
			h(key, value, tmp)
		}
		if tmp.batch != nil {
			err := this.db.Write(wo, tmp.batch)
			tmp.batch.Close()
			return err
		}
	}
	newkey := append(this.prefix, key...)
	return this.db.Put(wo, newkey, value)
}

func (this *DB) PutInBatch(key, value []byte, batch *levigo.WriteBatch) error {
	if len(this.hooks) != 0 {
		tmp := &tmpHook{db: this, key: key, value: value, batch: batch}
		for _, h := range this.hooks {
			h(key, value, tmp)
		}
	}
	newkey := append(this.prefix, key...)
	batch.Put(newkey, value)
	return nil
}

func (this *DB) Write(wo *levigo.WriteOptions, w *WriteBatch) error {
	return this.db.Write(wo, w.batch)
}

func (this *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{db: this, batch: levigo.NewWriteBatch(), prefix: this.prefix}
}

/**************************************************************
 *
 * sublevelIterator
 *
 **************************************************************/

func (this *sublevelIterator) Close() {
	this.it.Close()
}

func (this *sublevelIterator) GetError() error {
	return this.it.GetError()
}

func (this *sublevelIterator) Key() []byte {
	if !this.valid {
		return nil
	}
	return this.it.Key()[len(this.prefix):]
}

func (this *sublevelIterator) Next() {
	if !this.valid {
		return
	}
	this.it.Next()
	this.checkValid()
}

func (this *sublevelIterator) Prev() {
	if !this.valid {
		return
	}
	this.it.Prev()
	this.checkValid()
}

func (this *sublevelIterator) Seek(key []byte) {
	newkey := append(this.prefix, key...)
	this.it.Seek(newkey)
	this.checkValid()
}

func (this *sublevelIterator) SeekToFirst() {
	this.it.Seek(this.prefix)
	this.checkValid()
}

func (this *sublevelIterator) SeekToLast() {
	lastkey := make([]byte, len(this.prefix))
	copy(lastkey, this.prefix)
	lastkey[len(this.prefix)-1] = 1
	this.it.Seek(lastkey)
	// If the last row of this sublevel is the last row of the DB, then the previous seek
	// results in an invalid position
	if !this.it.Valid() {
		this.it.SeekToLast()
	} else {
		this.it.Prev()
	}
	this.checkValid()
}

func (this *sublevelIterator) Valid() bool {
	return this.valid && this.it.Valid()
}

func (this *sublevelIterator) Value() []byte {
	return this.it.Value()
}

func (this *sublevelIterator) checkValid() {
	if !this.it.Valid() {
		this.valid = false
		return
	}
	key := this.it.Key()
	if len(key) < len(this.prefix) {
		this.valid = false
		return
	}
	for i, v := range this.prefix {
		if v != key[i] {
			this.valid = false
			return
		}
	}	
	this.valid = true
}

/**************************************************************
 *
 * WriteBatch
 *
 **************************************************************/

func (this *WriteBatch) Clear() {
	this.batch.Clear()
}

func (this *WriteBatch) Close() {
	this.batch.Close()
}

func (this *WriteBatch) Delete(key []byte) {
	newkey := append(this.prefix, key...)
	this.batch.Delete(newkey)
	if len(this.db.hooks) != 0 {
		tmp := &tmpBatchHook{db: this.db, batch: this.batch}
		for _, h := range this.db.hooks {
			h(key, nil, tmp)
		}
	}
}

func (this *WriteBatch) Put(key, value []byte) {
	if len(this.db.hooks) != 0 {
		tmp := &tmpBatchHook{db: this.db, batch: this.batch}
		for _, h := range this.db.hooks {
			h(key, value, tmp)
		}
	}
	newkey := append(this.prefix, key...)
	this.batch.Put(newkey, value)	
}

/**************************************************************
 *
 * tmpHook
 *
 **************************************************************/

func (this *tmpHook) Delete(key []byte, sublevel *DB) {
	this.ensureBatch()
	if sublevel != nil {
		sublevel.DeleteInBatch(key, this.batch)
	} else {
		newkey := append(this.db.prefix, key...)
		this.batch.Delete(newkey)
	}
}

func (this *tmpHook) Put(key, value []byte, sublevel *DB) {
	this.ensureBatch()
	if sublevel != nil {
		sublevel.PutInBatch(key, value, this.batch)
	} else {
		newkey := append(this.db.prefix, key...)
		this.batch.Put(newkey, value)	
	}
}

func (this *tmpHook) ensureBatch() {
	if this.batch != nil {
		return
	}
	this.batch = levigo.NewWriteBatch()
	newkey := append(this.db.prefix, this.key...)
	if this.value == nil {
		this.batch.Delete(newkey)
	} else {
		this.batch.Put(newkey, this.value)
	}
}

/**************************************************************
 *
 * tmpBatchHook
 *
 **************************************************************/

func (this *tmpBatchHook) Delete(key []byte, sublevel *DB) {
	if sublevel != nil {
		sublevel.DeleteInBatch(key, this.batch)
	} else {
		newkey := append(this.db.prefix, key...)
		this.batch.Delete(newkey)
	}
}

func (this *tmpBatchHook) Put(key, value []byte, sublevel *DB) {
	if sublevel != nil {
		sublevel.PutInBatch(key, value, this.batch)
	} else {
		newkey := append(this.db.prefix, key...)
		this.batch.Put(newkey, value)	
	}
}
