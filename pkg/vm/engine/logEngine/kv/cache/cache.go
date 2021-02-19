package cache

import (
	"container/list"
	"io/ioutil"
	"matrixbase/pkg/logger"
	"matrixbase/pkg/mempool"
	"os"
	"path"

	aio "github.com/traetox/goaio"
)

func New(limit int64, path string, log logger.Log) (*Cache, error) {
	if fi, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.Mkdir(path, os.FileMode(0755)); err != nil {
			return nil, err
		}
	} else if !fi.IsDir() {
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		if err := os.Mkdir(path, os.FileMode(0755)); err != nil {
			return nil, err
		}
	}
	fs, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	c := &Cache{
		path:  path,
		limit: limit,
		lt:    list.New(),
		mp:    make(map[string]*list.Element),
	}
	for _, f := range fs {
		c.set(f.Name(), f.Size())
	}
	return c, nil
}

func (c *Cache) Del(k string) error {
	c.Lock()
	err := c.del(k)
	c.Unlock()
	return err
}

func (c *Cache) Add(k string, v []byte) error {
	if err := ioutil.WriteFile(path.Join(c.path, k), v, os.FileMode(0666)); err != nil {
		return err
	}
	c.Lock()
	err := c.set(k, int64(len(v)))
	c.Unlock()
	return err
}

func (c *Cache) Set(k string, s int64) error {
	c.Lock()
	err := c.set(k, s)
	c.Unlock()
	return err
}

func (c *Cache) Get(k string, mp *mempool.Mempool) ([]byte, *aio.AIO, aio.RequestId, bool, error) {
	c.RLock()
	v, a, id, ok, err := c.get(k, mp)
	c.RUnlock()
	return v, a, id, ok, err
}

func (c *Cache) del(k string) error {
	if e, ok := c.mp[k]; ok {
		c.size -= e.Value.(*entry).s
		c.lt.Remove(e)
		delete(c.mp, k)
		return os.Remove(path.Join(c.path, k))
	}
	return nil
}

func (c *Cache) get(k string, mp *mempool.Mempool) ([]byte, *aio.AIO, aio.RequestId, bool, error) {
	if e, ok := c.mp[k]; ok {
		c.lt.MoveToFront(e)
		data, a, id, err := readFile(path.Join(c.path, k), mp)
		return data, a, id, true, err

	}
	return nil, nil, 0, false, nil
}

func (c *Cache) set(k string, s int64) error {
	if e, ok := c.mp[k]; ok {
		c.lt.MoveToFront(e)
		{
			et := e.Value.(*entry)
			c.size += s - et.s
			et.s = s
		}
		return nil
	}
	c.mp[k] = c.lt.PushFront(&entry{s, k})
	if c.size += s; c.size > c.limit {
		c.reduce()
	}
	return nil
}

func (c *Cache) reduce() {
	for e := c.lt.Back(); e != nil; e = c.lt.Back() {
		if c.size < c.limit {
			return
		}
		et := e.Value.(*entry)
		c.size -= et.s
		if err := os.Remove(path.Join(c.path, et.k)); err != nil {
			c.log.Warnf("Failed to remove file '%s': %v", et.k, err)
		}
		delete(c.mp, et.k)
		c.lt.Remove(e)
	}
}

func readFile(name string, mp *mempool.Mempool) ([]byte, *aio.AIO, aio.RequestId, error) {
	a, err := aio.NewAIO(name, os.O_RDONLY, 0666)
	if err != nil {
		return nil, nil, 0, err
	}
	fi, err := os.Stat(name)
	if err != nil {
		return nil, nil, 0, err
	}
	size := int(fi.Size())
	data := mp.Alloc(size)
	id, err := a.ReadAt(data, 0)
	if err != nil {
		a.Close()
		return nil, nil, 0, err
	}
	return data, a, id, nil
}
