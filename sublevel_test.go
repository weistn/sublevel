package sublevel

import (
	"testing"
	"fmt"
	"bytes"
	"github.com/jmhodges/levigo"
)

func TestSublevel(t *testing.T) {
	opts := levigo.NewOptions()
	levigo.DestroyDatabase("test.ldb", opts)
	// opts.SetCache(levigo.NewLRUCache(3<<30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open("test.ldb", opts)
	if err != nil {
		t.Fatal(err)
	}

	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()

	sub1 := Sublevel(db, "foo")
	sub2 := Sublevel(db, "bar")

	err = sub1.Put(wo, []byte("hudel0000"), []byte("Test1_0000"))
	if err != nil {
		t.Fatal(err)
	}
	err = sub2.Put(wo, []byte("hudel0000"), []byte("Test2_0000"))
	if err != nil {
		t.Fatal(err)
	}

	val1, err := sub1.Get(ro, []byte("hudel0000"))
	if err != nil || string(val1) != "Test1_0000" {
		t.Fatal(err, string(val1))
	}
	val2, err := sub2.Get(ro, []byte("hudel0000"))
	if err != nil || string(val2) != "Test2_0000" {
		t.Fatal(err)
	}
	for i := 1; i < 100; i++ {
		err = sub1.Put(wo, []byte("hudel" + fmt.Sprintf("%04d", i)), []byte("Test1_" + fmt.Sprintf("%04d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i < 100; i++ {
		err = sub2.Put(wo, []byte("hudel" + fmt.Sprintf("%04d", i)), []byte("Test2_" + fmt.Sprintf("%04d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	it := sub1.NewIterator(ro)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if "hudel" + fmt.Sprintf("%04d", count) != string(it.Key()) {
			t.Fatal(string(it.Key()))
		}
		if "Test1_" + fmt.Sprintf("%04d", count) != string(it.Value()) {
			t.Fatal(string(it.Value()))
		}
		val, err := sub1.Get(ro, []byte("hudel" + fmt.Sprintf("%04d", count)))
		if err != nil || string(val) != "Test1_" + fmt.Sprintf("%04d", count) {
			t.Fatal(err, count, string(val))
		}		
		count++
	}
	if count != 100 {
		t.Fatalf("Iterator stopped too early or too late %v", count)
	}

	count = 99
	for it.SeekToLast(); it.Valid(); it.Prev() {
		if "hudel" + fmt.Sprintf("%04d", count) != string(it.Key()) {
			t.Fatal(string(it.Key()))
		}
		if "Test1_" + fmt.Sprintf("%04d", count) != string(it.Value()) {
			t.Fatal(string(it.Value()))
		}
		val, err := sub1.Get(ro, []byte("hudel" + fmt.Sprintf("%04d", count)))
		if err != nil || string(val) != "Test1_" + fmt.Sprintf("%04d", count) {
			t.Fatal(err, count, string(val))
		}		
		count--
	}
	if count != -1 {
		t.Fatalf("Iterator stopped too early or too late %v", count)
	}
	it.Close()

	count = 0
	it = sub2.NewIterator(ro)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		val, err := sub2.Get(ro, []byte("hudel" + fmt.Sprintf("%04d", count)))
		if err != nil || string(val) != "Test2_" + fmt.Sprintf("%04d", count) {
			t.Fatal(err)
		}		
		count++
	}
	if count != 100 {
		t.Fatalf("Iterator stopped too early %v", count)
	}
	it.Close()

	ro.Close()
	wo.Close()
	db.Close()
}

func TestHook(t *testing.T) {
	opts := levigo.NewOptions()
	levigo.DestroyDatabase("test.ldb", opts)
	// opts.SetCache(levigo.NewLRUCache(3<<30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open("test.ldb", opts)
	if err != nil {
		t.Fatal(err)
	}

	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()

	sub1 := Sublevel(db, "input")
	sub2 := Sublevel(db, "output")
	sub3 := Sublevel(db, "output2")
	sub1.Pre(func(key, value []byte, hook *Hook) {
		if value != nil {
			hook.Put(append([]byte("D_"), key...), value, sub2)	
		} else {
			hook.Delete(append([]byte("D_"), key...), sub2)			
		}
	})
	sub2.Pre(func(key, value []byte, hook *Hook) {
		if value != nil {
			hook.Put(append([]byte("X_"), key...), value, sub3)	
		} else {
			hook.Delete(append([]byte("X_"), key...), sub3)		
		}
	})

	post1Called := 0
	post3Called := 0

	sub1.Post(func(key, value []byte, hook *Hook) {
		post1Called++
		val, err := sub1.Get(ro, key)
		if err != nil || bytes.Compare(val, value) != 0 {
			t.Fatal(err, string(val), string(value))
		}		
	})

	sub3.Post(func(key, value []byte, hook *Hook) {
		post3Called++
		val, err := sub3.Get(ro, key)
		if err != nil || bytes.Compare(val, value) != 0 {
			t.Fatal(err, "read=", string(val), "hook=", string(value))
		}		
	})

	sub1.Put(wo, []byte("Name"), []byte("Horst"))
	val, err := sub1.Get(ro, []byte("Name"))
	if err != nil || string(val) != "Horst" {
		t.Fatal(err, string(val))
	}
	val, err = sub2.Get(ro, []byte("D_Name"))
	if err != nil || string(val) != "Horst" {
		t.Fatal(err, string(val))
	}
	val, err = sub3.Get(ro, []byte("X_D_Name"))
	if err != nil || string(val) != "Horst" {
		t.Fatal(err, string(val))
	}

	sub1.Delete(wo, []byte("Name"))
	val, err = sub1.Get(ro, []byte("Name"))
	if err != nil || val != nil {
		t.Fatal(err, string(val))
	}
	val, err = sub2.Get(ro, []byte("D_Name"))
	if err != nil || val != nil {
		t.Fatal(err, string(val))
	}
	val, err = sub3.Get(ro, []byte("X_D_Name"))
	if err != nil || val != nil {
		t.Fatal(err, string(val))
	}

	sub1.Put(wo, []byte("Color"), []byte("Red"))
	batch := sub1.NewWriteBatch()
	batch.Put([]byte("Sound"), []byte("Ping"))
	batch.Delete([]byte("Color"))
	sub1.Write(wo, batch)

	if post1Called != 5 {
		t.Fatal(post1Called)
	}
	if post3Called != 5 {
		t.Fatal(post3Called)
	}

	val, err = sub1.Get(ro, []byte("Color"))
	if err != nil || val != nil {
		t.Fatal(err, string(val))
	}
	val, err = sub2.Get(ro, []byte("D_Color"))
	if err != nil || val != nil {
		t.Fatal(err, string(val))
	}
	val, err = sub3.Get(ro, []byte("X_D_Color"))
	if err != nil || val != nil {
		t.Fatal(err, string(val))
	}

	val, err = sub1.Get(ro, []byte("Sound"))
	if err != nil || string(val) != "Ping" {
		t.Fatal(err, string(val))
	}
	val, err = sub2.Get(ro, []byte("D_Sound"))
	if err != nil || string(val) != "Ping" {
		t.Fatal(err, string(val))
	}
	val, err = sub3.Get(ro, []byte("X_D_Sound"))
	if err != nil || string(val) != "Ping" {
		t.Fatal(err, string(val))
	}

	ro.Close()
	wo.Close()
	db.Close()
}
