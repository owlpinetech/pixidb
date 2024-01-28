package pixidb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	"golang.org/x/exp/maps"
)

// TODO: consider using DirectIO for page reads? https://github.com/ncw/directio/blob/master/direct_io.go

// 4 bytes for int32 checksum in each page
const ChecksumSize int = 4

// Wrapper struct for a page that has been loaded into memory. Contains
// a 'dirty' flag to mark the cached page as having received an update
// in the data that needs to be flushed to disk.
type Page struct {
	data  []byte
	dirty bool
}

// Abstracts the data access and caching in memory of a large file using
// a fixed page size. Individual operations intended to be threadsafe and
// allow for concurrency while maintaining efficiency. This abstraction
// also implements basic checksumming to validate the integrity of the data.
// A checksum is stored just before each page in the disk file, but this piece
// of data is not included in the slices returned by any of the public facing
// methods of this type.
// https://en.wikipedia.org/wiki/The_Pagemaster
type Pagemaster struct {
	maxCache int
	cache    map[int]*Page
	lock     sync.RWMutex
	path     string
	pageSize int
}

// Create a new cached data layer to access the file on disk location at `path`, with
// the specified number of pages allowed in the cache. No disk side effect. Must call
// Initialize afterward if the path is to a newly created (empty) file.
func NewPagemaster(path string, maxCache int) *Pagemaster {
	return &Pagemaster{
		maxCache,
		make(map[int]*Page),
		sync.RWMutex{},
		path,
		os.Getpagesize() - ChecksumSize,
	}
}

// For pagemasters created over newly created empty files, this function will initialize
// the file with the given number of pages, each page filled with the same given template
// of data. If a write to the file fails, all of the writes that have succeeded to that
// point will not be undone. However, future calls to Initialize (e.g. a rety), will write
// over any data that was written previously.
func (p *Pagemaster) Initialize(pages int, page []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	file, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	for i := 0; i < pages; i++ {
		if err := p.writePage(file, i, page); err != nil {
			return err
		}
	}
	return nil
}

// The number of bytes that be written to per page in the file.
func (p *Pagemaster) PageSize() int {
	return p.pageSize
}

// The maximum number of pages allowed in the cache.
func (p *Pagemaster) MaxPagesInCache() int {
	return p.maxCache
}

// The current number of pages in the cache.
func (p *Pagemaster) PagesInCache() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.cache)
}

// Empties the cache of all pages. Does not destroy the data in the pages,
// so if those are still referenced elsewhere they will not be garbage collected.
// No disk side effect.
func (p *Pagemaster) ClearCache() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.cache = make(map[int]*Page)
}

// Retrieve the page at the given index from disk, load it into the cache, and
// return the data. Always skips cache to read from disk. If the cache is full,
// a different page is removed from the cache before the requested page is added.
func (p *Pagemaster) LoadPage(pageIndex int) ([]byte, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	page, err := p.loadPage(pageIndex)
	if err != nil {
		return nil, err
	}
	return page.data, nil
}

// Get the page with the sequential index given. If the page exists in the cache,
// does not access the disk. Otherwise, loads the page into the cache and returns it.
func (p *Pagemaster) GetPage(pageIndex int) ([]byte, error) {
	p.lock.RLock()
	cached, ok := p.cache[pageIndex]
	p.lock.RUnlock()

	if ok {
		return cached.data, nil
	}

	page, err := p.LoadPage(pageIndex)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// Essentially the same actions as GetPage, but returns a portion of the page data
// at the given byte offset.
func (p *Pagemaster) GetChunk(pageIndex int, offset int, size int) ([]byte, error) {
	page, err := p.GetPage(pageIndex)
	if err != nil {
		return nil, err
	}

	return page[offset : offset+size], nil
}

// Sets the data for the page at the given index, and marks the cache entry as dirty.
// If the page does not yet exist in the cache, it will exist in the cache afterwards,
// potentially unloading a different page to make room.
func (p *Pagemaster) SetPage(pageIndex int, page []byte) error {
	// make sure to keep the cache under the max, GetPage does the trick
	_, err := p.GetPage(pageIndex)
	if err != nil {
		return err
	}

	p.lock.Lock()
	defer p.lock.Unlock()
	p.cache[pageIndex] = &Page{page, true}
	return nil
}

// Similar to SetPage but only updates the specified portion of data in the page.
func (p *Pagemaster) SetChunk(pageIndex int, offset int, chunk []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	page, err := p.getPage(pageIndex)
	if err != nil {
		return err
	}

	copy(page.data[offset:], chunk)
	page.dirty = true
	return nil
}

// Writes the page in the cache to disk, whether it is dirty or not. Marks
// the page as clean afterward. If the page does not exist in the cache, no
// action is taken. If the write is unsuccessful, the page dirtiness status
// will be left unchanged.
func (p *Pagemaster) FlushPage(pageIndex int) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	page, ok := p.cache[pageIndex]
	if !ok {
		return nil
	}
	err := p.openAndWritePage(pageIndex, page.data)
	if err == nil {
		page.dirty = true
	}
	return err
}

// Writes all pages marked dirty to the disk, locking access to the cache and
// the file until writing is complete. If a page write files, the process is stopped
// and an error is returned, but only the successfully written pages will be marked
// clean. The page on which the write errored, and the remaining dirty pages, will
// still be marked dirty if the managing process wants to retry flushing.
func (p *Pagemaster) FlushAllPages() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	for id, page := range p.cache {
		if page.dirty {
			err := p.openAndWritePage(id, page.data)
			if err != nil {
				return err
			}
			page.dirty = false
		}
	}
	return nil
}

func (p *Pagemaster) loadPage(pageIndex int) (*Page, error) {
	if page, ok := p.cache[pageIndex]; ok {
		return page, nil
	}

	// page not present in cache, get it from disk
	pageData, err := p.readPage(pageIndex)
	if err != nil {
		return nil, err
	}
	// load page into cache, clearing out room if necessary
	if len(p.cache) > p.maxCache {
		remPage := maps.Keys(p.cache)[0]
		p.openAndWritePage(remPage, p.cache[remPage].data)
		// TODO: make this into LRU/LFU/ARC cache to reduce nondeterministic thrashing
		delete(p.cache, remPage)
	}
	p.cache[pageIndex] = &Page{pageData, false}
	return p.cache[pageIndex], nil
}

func (p *Pagemaster) getPage(pageIndex int) (*Page, error) {
	cached, ok := p.cache[pageIndex]

	if ok {
		return cached, nil
	}

	page, err := p.loadPage(pageIndex)
	if err != nil {
		return nil, err
	}
	return page, nil
}

func (p *Pagemaster) openAndWritePage(pageIndex int, page []byte) error {
	file, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	return p.writePage(file, pageIndex, page)
}

func (p *Pagemaster) writePage(file *os.File, pageIndex int, page []byte) error {
	if len(page) < p.pageSize {
		fill := make([]byte, p.pageSize-len(page))
		page = append(page, fill...)
	}

	checksum := crc32.ChecksumIEEE(page)
	offset := int64(pageIndex) * int64(p.pageSize+ChecksumSize)
	encoded := make([]byte, ChecksumSize)
	binary.BigEndian.PutUint32(encoded, checksum)
	if _, err := file.WriteAt(encoded, offset); err != nil {
		return err
	}
	if _, err := file.WriteAt(page, offset+int64(ChecksumSize)); err != nil {
		return err
	}
	return nil
}

func (p *Pagemaster) readPage(pageIndex int) ([]byte, error) {
	file, err := os.Open(p.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	offset := int64(pageIndex) * int64(p.pageSize+ChecksumSize)
	page := make([]byte, p.pageSize+ChecksumSize)
	if _, err := file.ReadAt(page, offset); err != nil {
		return nil, err
	}
	savedChecksum := binary.BigEndian.Uint32(page)
	if savedChecksum != crc32.ChecksumIEEE(page[ChecksumSize:]) {
		// TODO: move this error into an ERRORS file
		return nil, fmt.Errorf("pixidb: Database read revealed corrupted data on a page")
	}
	return page[ChecksumSize:], nil
}
