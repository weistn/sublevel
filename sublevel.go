package sublevel

import (
	"github.com/jmhodges/levigo"
	"bytes"
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

/*
type Hook interface {
	Delete(key []byte, sublevel *DB)
	Put(key, value []byte, sublevel *DB)
}
*/

type HookFunc func(key, value []byte, hook *Hook)

type WriteBatch struct {
	db *DB
	batch *levigo.WriteBatch
	hook *Hook
}

type DB struct {
	db *levigo.DB
	prefixStr string
	prefix []byte
	pre []HookFunc
	post []HookFunc
}

type sublevelIterator struct {
	it *levigo.Iterator
	prefix []byte
	valid bool
}

type KeyValue struct {
	key []byte
	value []byte
}

type Hook struct {
	db *DB
	batch *levigo.WriteBatch
	key []byte
	value []byte
	sublevels map[string]*DB
	kv []KeyValue
}

/**************************************************************
 *
 * sublevelDB
 *
 **************************************************************/

func Sublevel(db *levigo.DB, prefix string) *DB {
	return &DB{db: db, prefix: append([]byte(prefix),0), prefixStr: prefix}
}

func (this *DB) LevelDB() *levigo.DB {
	return this.db
}

func (this *DB) Pre(hook HookFunc) {
	this.pre = append(this.pre, hook)
}

func (this *DB) Post(hook HookFunc) {
	this.post = append(this.post, hook)
}

func (this *DB) Delete(wo *levigo.WriteOptions, key []byte) (err error) {
	var hook *Hook
	if len(this.pre) != 0 || len(this.post) != 0 {
		hook = &Hook{db: this, key: key, value: nil}
		for _, h := range this.pre {
			h(key, nil, hook)
		}
	}
	if hook != nil && hook.batch != nil {
		err = this.db.Write(wo, hook.batch)
		hook.batch.Close()
	} else {
		newkey := append(this.prefix, key...)
		err = this.db.Delete(wo, newkey)
	}
	if err != nil {
		// TODO: Unlock something?
		return
	}
	if hook != nil {
		this.runPost(hook)
		if hook.sublevels != nil {
			for _, s := range hook.sublevels {
				s.runPost(hook)
			}
		}
	}
	return
}

func (this *DB) deleteInHook(key []byte, hook *Hook) error {
	newkey := append(this.prefix, key...)
	hook.kv = append(hook.kv, KeyValue{newkey, nil})
	if len(this.pre) != 0 {
		for _, h := range this.pre {
			h(key, nil, hook)
		}
	}
	if len(this.post) != 0 {
		hook.addSublevel(this)
	}
	hook.batch.Delete(newkey)
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

func (this *DB) Put(wo *levigo.WriteOptions, key, value []byte) (err error) {
//	println("put start", string(key), string(value))
	var hook *Hook
	if len(this.pre) != 0 || len(this.post) != 0 {
		hook = &Hook{db: this, key: key, value: value}
		for _, h := range this.pre {
			h(key, value, hook)
		}
	}
	if hook != nil && hook.batch != nil {
		err = this.db.Write(wo, hook.batch)
		hook.batch.Close()
	} else {
		newkey := append(this.prefix, key...)
		err = this.db.Put(wo, newkey, value)
	}
	if err != nil {
		// TODO: Unlock something?
		return
	}
	if hook != nil {
		this.runPost(hook)
		if hook.sublevels != nil {
			for _, s := range hook.sublevels {
				s.runPost(hook)
			}
		}
	}
//	println("put end", string(key), string(value))
	return
}

func (this *DB) putInHook(key, value []byte, hook *Hook) error {
	newkey := append(this.prefix, key...)
	hook.kv = append(hook.kv, KeyValue{newkey, value})
	if len(this.pre) != 0 {
		for _, h := range this.pre {
			h(key, value, hook)
		}
	}
	if len(this.post) != 0 {
		hook.addSublevel(this)
	}
	hook.batch.Put(newkey, value)
	return nil
}

func (this *DB) NewWriteBatch() *WriteBatch {
	batch := levigo.NewWriteBatch()
	var hook *Hook
	if len(this.pre) != 0 || len(this.post) != 0 {
		hook = &Hook{db: this, batch: batch}
	}
	return &WriteBatch{db: this, batch: batch, hook: hook}
}

func (this *DB) Write(wo *levigo.WriteOptions, w *WriteBatch) (err error) {
	err = this.db.Write(wo, w.batch)
	if err != nil {
		// TODO: Unlock something?
		return
	}
	if w.hook != nil {
		this.runPost(w.hook)
		if w.hook.sublevels != nil {
			for _, s := range w.hook.sublevels {
				s.runPost(w.hook)
			}
		}		
	}
	return
}

func (this *DB) Simulate(wo *levigo.WriteOptions, key, value []byte) (err error) {
	var hook *Hook
	if len(this.pre) != 0 || len(this.post) != 0 {
		hook = &Hook{db: this, key: key, value: value}
		for _, h := range this.pre {
			h(key, value, hook)
		}
	}
	if hook != nil && hook.batch != nil {
		err = this.db.Write(wo, hook.batch)
		hook.batch.Close()
	}
	if hook != nil {
		this.runPost(hook)
		if hook.sublevels != nil {
			for _, s := range hook.sublevels {
				s.runPost(hook)
			}
		}
	}
	return
}

func (this *DB) runPost(hook *Hook) {
	if len(this.post) != 0 {
		if hook.batch != nil {
			for _, kv := range hook.kv {
				if bytes.HasPrefix(kv.key, this.prefix) {
					for _, h := range this.post {
						h(kv.key[len(this.prefix):], kv.value, hook)
					}
				}
			}
		} else if (hook.db == this && hook.key != nil) {
			for _, h := range this.post {
				h(hook.key, hook.value, hook)
			}
		}
	}
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

//func (this *WriteBatch) Clear() {
//	this.batch.Clear()
//}

func (this *WriteBatch) Close() {
	this.batch.Close()
}

func (this *WriteBatch) Delete(key []byte) {
	if this.hook != nil {
		this.db.deleteInHook(key, this.hook)
	} else {
		newkey := append(this.db.prefix, key...)
		this.batch.Delete(newkey)
	}
}

func (this *WriteBatch) Put(key, value []byte) {
	if this.hook != nil {
		this.db.putInHook(key, value, this.hook)
	} else {
		newkey := append(this.db.prefix, key...)
		this.batch.Put(newkey, value)
	}
}

/**************************************************************
 *
 * Hook
 *
 **************************************************************/

func (this *Hook) Delete(key []byte, sublevel *DB) {
	this.ensureBatch()
	if sublevel != nil {
		sublevel.deleteInHook(key, this)
	} else {
		newkey := append(this.db.prefix, key...)
		this.batch.Delete(newkey)
	}
}

func (this *Hook) Put(key, value []byte, sublevel *DB) {
	this.ensureBatch()
	if sublevel != nil {
		sublevel.putInHook(key, value, this)
	} else {
		newkey := append(this.db.prefix, key...)
		this.batch.Put(newkey, value)	
	}
}

func (this *Hook) ensureBatch() {
	if this.batch != nil {
		return
	}
	this.batch = levigo.NewWriteBatch()
	if this.key != nil {
		newkey := append(this.db.prefix, this.key...)
		if this.value == nil {
			this.batch.Delete(newkey)
		} else {
			this.batch.Put(newkey, this.value)
		}
		this.kv = []KeyValue{KeyValue{newkey, this.value}}
		this.key = nil
		this.value = nil
	}
}

func (this *Hook) addSublevel(sublevel *DB) {
	if this.db == sublevel {
		return
	}
	if this.sublevels == nil {
		this.sublevels = make(map[string]*DB)
		this.sublevels[sublevel.prefixStr] = sublevel
	} else if _, ok := this.sublevels[sublevel.prefixStr]; !ok {
		this.sublevels[sublevel.prefixStr] = sublevel		
	}
}
