package chunkserver

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

func chunkFilename(dataDir, handle string) string {
	return filepath.Join(dataDir, fmt.Sprintf("chunk_%s.chk", handle))
}

func ensureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

func writeAt(dataDir, handle string, offset int64, data []byte) error {
	if err := ensureDir(dataDir); err != nil {
		return err
	}
	fpath := chunkFilename(dataDir, handle)
	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return nil
}

// optional checksum for block
func blockChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// read length at offset
func getSize(dataDir, handle string) (int64, error) {
	fpath := chunkFilename(dataDir, handle)
	fi, err := os.Stat(fpath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return fi.Size(), nil
}

func deleteChunkFile(dataDir, handle string) error {
	fpath := chunkFilename(dataDir, handle)
	if err := os.Remove(fpath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

func readAt(dataDir, handle string, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, fmt.Errorf("invalid offset")
	}
	fpath := chunkFilename(dataDir, handle)
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if offset > fi.Size() {
		return []byte{}, nil
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	var readLen int64
	if length <= 0 || offset+length > fi.Size() {
		readLen = fi.Size() - offset
	} else {
		readLen = length
	}
	buf := make([]byte, readLen)
	n, err := io.ReadFull(f, buf)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return buf[:n], nil
		}
		return nil, err
	}
	return buf, nil
}

func writeZeroAt(dataDir, handle string, offset int64, length int64) error {
	if length <= 0 {
		return nil
	}
	if err := ensureDir(dataDir); err != nil {
		return err
	}
	fpath := chunkFilename(dataDir, handle)
	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	const zeroBufSize = 64 * 1024
	buf := make([]byte, zeroBufSize)
	remaining := length
	for remaining > 0 {
		chunk := int64(len(buf))
		if remaining < chunk {
			chunk = remaining
		}
		if _, err := f.Write(buf[:int(chunk)]); err != nil {
			return err
		}
		remaining -= chunk
	}
	return nil
}

func truncateChunkFile(dataDir, handle string, size int64) error {
	if size < 0 {
		size = 0
	}
	if err := ensureDir(dataDir); err != nil {
		return err
	}
	fpath := chunkFilename(dataDir, handle)
	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return nil
}
