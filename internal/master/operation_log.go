package master

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type opRecord struct {
	Op        string   `json:"op"`
	File      string   `json:"file,omitempty"`
	NewName   string   `json:"new_name,omitempty"`
	Chunk     string   `json:"chunk,omitempty"`
	ChunkList []string `json:"chunk_list,omitempty"`
}

type OperationLog struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func NewOperationLog(path string) (*OperationLog, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &OperationLog{f: f, w: bufio.NewWriter(f)}, nil
}

func (ol *OperationLog) Append(op opRecord) error {
	ol.mu.Lock()
	defer ol.mu.Unlock()
	b, err := json.Marshal(op)
	if err != nil {
		return err
	}
	if _, err := ol.w.Write(b); err != nil {
		return err
	}
	if _, err := ol.w.Write([]byte("\n")); err != nil {
		return err
	}
	return ol.w.Flush()
}

func (ol *OperationLog) Close() error {
	ol.mu.Lock()
	defer ol.mu.Unlock()
	if ol.w != nil {
		if err := ol.w.Flush(); err != nil {
			return err
		}
	}
	if ol.f != nil {
		err := ol.f.Close()
		ol.f = nil
		ol.w = nil
		return err
	}
	return nil
}

func (ol *OperationLog) Replay(path string, meta *MasterMetadata) error {
	f, err := os.Open(path)
	if err != nil {
		return nil
	} // no log to replay
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r opRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			continue
		}
		switch r.Op {
		case "create":
			meta.SetFileChunks(r.File, r.ChunkList)
			for _, c := range r.ChunkList {
				meta.EnsureChunk(c)
			}
		case "delete":
			meta.mu.Lock()
			delete(meta.FileMap, r.File)
			meta.mu.Unlock()
		case "rename":
			meta.mu.Lock()
			if chunks, ok := meta.FileMap[r.File]; ok {
				meta.FileMap[r.NewName] = chunks
				delete(meta.FileMap, r.File)
			}
			meta.mu.Unlock()
		case "add_chunk":
			meta.mu.Lock()
			chunks := meta.FileMap[r.File]
			chunks = append(chunks, r.Chunk)
			meta.FileMap[r.File] = chunks
			meta.mu.Unlock()
			meta.EnsureChunk(r.Chunk)
		}
	}
	return nil
}
