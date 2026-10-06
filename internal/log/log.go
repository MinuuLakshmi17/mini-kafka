package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// On-disk record layout: 8-byte offset | 8-byte unix-nano timestamp |
// 4-byte payload length | payload bytes.
const headerSize = 8 + 8 + 4

// Record is a single committed log entry.
type Record struct {
	Offset int64
	UnixNS int64
	Value  []byte
}

// indexEntry maps an offset to its byte position inside a segment file.
type indexEntry struct {
	Offset   int64
	Position int64
}

// Partition is one append-only, segmented log.
type Partition struct {
	mu           sync.RWMutex
	dir          string
	segmentBytes int64
	indexEvery   int64
	nextOffset   int64
	file         *os.File // active segment
	fileStart    int64
	position     int64 // valid bytes in the active segment
	segRecs      int64 // records in the active segment
	segStarts    []int64
	segIdx       map[int64][]indexEntry // segment start offset -> sparse index
	fsync        bool
}

// OpenPartition opens (or creates) the partition rooted at dir, recovering
// existing segments. Torn tail writes in the active segment are truncated.
func OpenPartition(dir string, segmentBytes, indexEvery int64, fsync bool) (*Partition, error) {
	if segmentBytes <= 0 {
		segmentBytes = 64 << 20
	}
	if indexEvery <= 0 {
		indexEvery = 16
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	p := &Partition{
		dir:          dir,
		segmentBytes: segmentBytes,
		indexEvery:   indexEvery,
		fsync:        fsync,
		segIdx:       make(map[int64][]indexEntry),
	}
	if err := p.recover(); err != nil {
		return nil, err
	}
	return p, nil
}

func parseSegmentStart(path string) int64 {
	var start int64
	fmt.Sscanf(filepath.Base(path), "%d.log", &start)
	return start
}

// scanSegment walks record headers, stopping at the first torn or
// out-of-sequence record. It returns the sparse index, the valid record
// count, and the valid byte size.
func (p *Partition) scanSegment(f *os.File, size, start int64) (index []indexEntry, recs int64, valid int64) {
	h := make([]byte, headerSize)
	var pos int64
	offset := start
	for pos+headerSize <= size {
		if _, err := f.ReadAt(h, pos); err != nil {
			break
		}
		off := int64(binary.BigEndian.Uint64(h[0:8]))
		l := int64(binary.BigEndian.Uint32(h[16:20]))
		if off != offset || l < 0 || pos+headerSize+l > size {
			break
		}
		if recs%p.indexEvery == 0 {
			index = append(index, indexEntry{off, pos})
		}
		recs++
		offset = off + 1
		pos += headerSize + l
	}
	return index, recs, pos
}

func (p *Partition) recover() error {
	entries, err := filepath.Glob(filepath.Join(p.dir, "*.log"))
	if err != nil {
		return err
	}
	sort.Strings(entries)
	if len(entries) == 0 {
		return p.newSegment(0)
	}
	for i, path := range entries {
		start := parseSegmentStart(path)
		f, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		st, _ := f.Stat()
		index, recs, valid := p.scanSegment(f, st.Size(), start)
		if i == len(entries)-1 {
			// Active segment: truncate any torn tail, keep writing.
			if valid != st.Size() {
				if err := f.Truncate(valid); err != nil {
					f.Close()
					return err
				}
			}
			// The file was opened without O_APPEND; position it at the
			// end so the next Append does not overwrite valid records.
			if _, err := f.Seek(0, io.SeekEnd); err != nil {
				f.Close()
				return err
			}
			p.file = f
			p.fileStart = start
			p.position = valid
			p.segRecs = recs
			p.nextOffset = start + recs
			p.segStarts = append(p.segStarts, start)
			p.segIdx[start] = index
			return nil
		}
		f.Close()
		p.segStarts = append(p.segStarts, start)
		p.segIdx[start] = index
		p.nextOffset = start + recs
	}
	return nil
}

func (p *Partition) newSegment(start int64) error {
	if p.file != nil {
		_ = p.file.Sync()
		_ = p.file.Close()
	}
	path := filepath.Join(p.dir, fmt.Sprintf("%020d.log", start))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	p.file = f
	p.fileStart = start
	p.position = 0
	p.segRecs = 0
	p.segStarts = append(p.segStarts, start)
	p.segIdx[start] = nil
	return nil
}

// Append adds one record, rolling to a new segment when the active one is
// full. It returns the record's offset.
func (p *Partition) Append(value []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if int64(headerSize+len(value))+p.position > p.segmentBytes && p.position > 0 {
		if err := p.newSegment(p.nextOffset); err != nil {
			return 0, err
		}
	}
	off := p.nextOffset
	now := time.Now().UnixNano()
	h := make([]byte, headerSize)
	binary.BigEndian.PutUint64(h[0:8], uint64(off))
	binary.BigEndian.PutUint64(h[8:16], uint64(now))
	binary.BigEndian.PutUint32(h[16:20], uint32(len(value)))
	if _, err := p.file.Write(h); err != nil {
		return 0, err
	}
	if _, err := p.file.Write(value); err != nil {
		return 0, err
	}
	if p.segRecs%p.indexEvery == 0 {
		p.segIdx[p.fileStart] = append(p.segIdx[p.fileStart], indexEntry{off, p.position})
	}
	p.position += int64(headerSize + len(value))
	p.segRecs++
	p.nextOffset++
	if p.fsync {
		if err := p.file.Sync(); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// ReadFrom returns up to max records starting at offset, using the sparse
// index to seek close to the requested offset before scanning.
func (p *Partition) ReadFrom(offset int64, max int) ([]Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if max <= 0 {
		max = 100
	}
	if offset < 0 {
		offset = 0
	}
	out := make([]Record, 0, max)
	if len(p.segStarts) == 0 {
		return out, nil
	}
	// Rightmost segment whose start offset <= requested offset.
	si := 0
	for i, s := range p.segStarts {
		if s <= offset {
			si = i
		} else {
			break
		}
	}
	for ; si < len(p.segStarts) && len(out) < max; si++ {
		start := p.segStarts[si]
		pos := int64(0)
		for _, e := range p.segIdx[start] {
			if e.Offset <= offset {
				pos = e.Position
			} else {
				break
			}
		}
		path := filepath.Join(p.dir, fmt.Sprintf("%020d.log", start))
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		size := p.segmentFileSize(si, f)
		h := make([]byte, headerSize)
		for pos+headerSize <= size && len(out) < max {
			if _, err := f.ReadAt(h, pos); err != nil {
				break
			}
			off := int64(binary.BigEndian.Uint64(h[:8]))
			ln := int64(binary.BigEndian.Uint32(h[16:20]))
			if ln < 0 || pos+headerSize+ln > size {
				break
			}
			if off >= offset {
				b := make([]byte, ln)
				if _, err := f.ReadAt(b, pos+headerSize); err != nil {
					f.Close()
					return nil, err
				}
				out = append(out, Record{off, int64(binary.BigEndian.Uint64(h[8:16])), b})
			}
			pos += headerSize + ln
		}
		f.Close()
	}
	return out, nil
}

// segmentFileSize returns the readable size of a segment: the tracked write
// position for the active segment, the file size for sealed ones.
func (p *Partition) segmentFileSize(si int, f *os.File) int64 {
	if si == len(p.segStarts)-1 {
		return p.position
	}
	st, _ := f.Stat()
	return st.Size()
}

// NextOffset returns the offset the next appended record will receive.
func (p *Partition) NextOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOffset
}

// Close flushes and closes the active segment file.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file == nil {
		return nil
	}
	_ = p.file.Sync()
	return p.file.Close()
}

var ErrNotFound = errors.New("not found")
