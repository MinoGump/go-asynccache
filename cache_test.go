package cache

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestGetOK(t *testing.T) {
	var key, ret = "key", "ret"
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return ret, nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	v, err := c.Get(key)
	Assert(t, err == nil)
	Assert(t, v.(string) == ret)

	time.Sleep(time.Second / 2)
	ret = "change"
	v, err = c.Get(key)
	Assert(t, err == nil)
	Assert(t, v.(string) != ret)

	time.Sleep(time.Second)
	v, err = c.Get(key)
	Assert(t, err == nil)
	Assert(t, v.(string) == ret)
}

func TestGetErr(t *testing.T) {
	var key, ret = "key", "ret"
	var first = true
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			if first {
				first = false
				return nil, errors.New("error")
			}
			return ret, nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	v, err := c.Get(key)
	Assert(t, err != nil)
	Assert(t, v == nil)

	time.Sleep(time.Second / 2)
	_, err2 := c.Get(key)
	Assert(t, err == err2)

	time.Sleep(time.Second)
	v, err = c.Get(key)
	Assert(t, err == nil)
	Assert(t, v.(string) == ret)
}

func TestGetOrSetOK(t *testing.T) {
	var key, ret, def = "key", "ret", "def"
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return ret, nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	v := c.GetOrSet(key, def)
	Assert(t, v.(string) == ret)

	time.Sleep(time.Second / 2)
	ret = "change"
	v = c.GetOrSet(key, def)
	Assert(t, v.(string) != ret)

	time.Sleep(time.Second)
	v = c.GetOrSet(key, def)
	Assert(t, v.(string) == ret)
}

func TestGetOrSetErr(t *testing.T) {
	var key, ret, def = "key", "ret", "def"
	var first = true
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			if first {
				first = false
				return nil, errors.New("error")
			}
			return ret, nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	v := c.GetOrSet(key, def)
	Assert(t, v.(string) == def)

	time.Sleep(time.Second / 2)
	v = c.GetOrSet(key, ret)
	Assert(t, v.(string) != ret)
	Assert(t, v.(string) == def)

	time.Sleep(time.Second)
	v = c.GetOrSet(key, def)
	Assert(t, v.(string) == ret)
}

func TestGetOrResetOK(t *testing.T) {
	var key, ret, def = "key", "ret", "def"
	op := Options{
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		DataFetcher: func(req interface{}) (interface{}, error) {
			return req, nil
		},
		EnableRefresh:  false,
		EnableExpire:   true,
		ExpireDuration: time.Second,
	}
	c := NewCache(op)

	v := c.GetOrReset(key, def)
	Assert(t, v.(string) == def)

	time.Sleep(time.Second / 2)
	ret = "change"
	v = c.GetOrReset(key, ret)
	Assert(t, v.(string) != ret)

	time.Sleep(2 * time.Second)
	v = c.GetOrReset(key, ret)
	Assert(t, v.(string) == ret)
}

func TestSetDefault(t *testing.T) {
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return nil, errors.New("error")
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	v := c.GetOrSet("key1", "def1")
	Assert(t, v.(string) == "def1")

	exist := c.SetDefault("key2", "val2")
	Assert(t, !exist)
	v = c.GetOrSet("key2", "def2")
	Assert(t, v.(string) == "val2")

	exist = c.SetDefault("key2", "val3")
	Assert(t, exist)
	v = c.GetOrSet("key2", "def2")
	Assert(t, v.(string) == "val2")
}

func TestDeleteIf(t *testing.T) {
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return nil, errors.New("error")
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	c.SetDefault("key", "val")
	v := c.GetOrSet("key", "def")
	Assert(t, v.(string) == "val")

	d, _ := c.(interface{ DeleteIf(func(key string) bool) })
	d.DeleteIf(func(string) bool { return true })

	v = c.GetOrSet("key", "def")
	Assert(t, v.(string) == "def")
}

func TestClose(t *testing.T) {
	var dur = time.Second / 10
	var cnt int
	op := Options{
		RefreshDuration: dur - time.Millisecond,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			cnt++
			return cnt, nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	v := c.GetOrSet("key", 10)
	Assert(t, v.(int) == 1)

	time.Sleep(dur)
	v = c.GetOrSet("key", 10)
	Assert(t, v.(int) == 2)

	time.Sleep(dur)
	v = c.GetOrSet("key", 10)
	Assert(t, v.(int) == 3)

	cc, _ := c.(interface{ Close() })
	cc.Close()

	time.Sleep(dur)
	v = c.GetOrSet("key", 10)
	Assert(t, v.(int) == 3)
}

func TestExpire(t *testing.T) {
	// trigger is used to mark whether fetcher is called
	trigger := false
	op := Options{
		EnableExpire:    true,
		ExpireDuration:  3 * time.Minute,
		RefreshDuration: time.Minute,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return true
		},
		Fetcher: func(key string) (interface{}, error) {
			trigger = true
			return "", nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op).(*cache)

	// GetOrSet cannot trigger fetcher when SetDefault before
	c.SetDefault("key-default", "")
	c.SetDefault("key-alive", "")
	c.GetOrSet("key-alive", "")
	Assert(t, trigger == false)

	c.Get("key-expire")
	Assert(t, trigger == true)

	// first expire set tag
	c.expire()

	trigger = false
	c.Get("key-alive")
	Assert(t, trigger == false)
	// second expire, both key-default & key-expire have been removed
	c.expire()
	c.refresh() // prove refresh does not affect expire

	trigger = false
	c.Get("key-alive")
	Assert(t, trigger == false)
	trigger = false
	c.Get("key-default")
	Assert(t, trigger == true)
	trigger = false
	c.Get("key-expire")
	Assert(t, trigger == true)
}

func BenchmarkGet(b *testing.B) {
	var key = "key"
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return "", nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(key)
	}
}

func BenchmarkGetParallel(b *testing.B) {
	var key = "key"
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return "", nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.Get(key)
		}
	})
}

func BenchmarkGetOrSet(b *testing.B) {
	var key, def = "key", "def"
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return "", nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetOrSet(key, def)
	}
}

func BenchmarkGetOrSetParallel(b *testing.B) {
	var key, def = "key", "def"
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return "", nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.GetOrSet(key, def)
		}
	})
}

func BenchmarkRefresh(b *testing.B) {
	var key, def = "key", "def"
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return "", nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op).(*cache)
	c.SetDefault(key, def)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.refresh()
	}
}

func BenchmarkRefreshParallel(b *testing.B) {
	var key, def = "key", "def"
	op := Options{
		RefreshDuration: time.Second,
		IsSame: func(key string, oldData, newData interface{}) bool {
			return false
		},
		Fetcher: func(key string) (interface{}, error) {
			return "", nil
		},
		EnableRefresh: true,
	}
	c := NewCache(op).(*cache)
	c.SetDefault(key, def)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.refresh()
		}
	})
}

// testingTB is a subset of common methods between *testing.T and *testing.B.
type testingTB interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Helper()
}

// Assert .
func Assert(t testingTB, cond bool) {
	t.Helper()
	if !cond {
		t.Fatal("assertion failed")
	}
}

// Assertf .
func Assertf(t testingTB, cond bool, format string, val ...interface{}) {
	t.Helper()
	if !cond {
		t.Fatalf(format, val...)
	}
}

// DeepEqual .
func DeepEqual(t testingTB, a, b interface{}) {
	t.Helper()
	if !reflect.DeepEqual(a, b) {
		t.Fatal("assertion failed")
	}
}
