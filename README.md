# go-asynccache

Golang library for async data loading and caching

### Example
---
```golang
opts := Options{
	EnableExpire:   true,
	ExpireDuration: 10 * time.Minute,
	DataFetcher: func(req interface{}) (interface{}, error) {
		// handler
		return resp, nil 
	},
	EnableRefresh: false,
}
cache = NewCache(opts)
resp, err := cache.GetOrReset(key, req)
```