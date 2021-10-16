package borshfile

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/mostynb/zstdpool-freelist"
)

type BorshFile struct {
	file *os.File
}

func NewBorshFile(path string) (*BorshFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	return &BorshFile{
		file: file,
	}, nil
}

func (bf *BorshFile) WriteBytes(buf []byte) (int, error) {
	return bf.file.Write(buf)
}

func (bf *BorshFile) WriteBytesFromReader(reader io.Reader) (int64, error) {
	return io.Copy(bf.file, reader)
}

func (bf *BorshFile) WriteUint32LE(v uint32) error {
	return WriteUint32LE(bf.file, v)
}

func (bf *BorshFile) WriteBorshSlice(buf []byte) (int, error) {
	// Write len:
	err := WriteUint32LE(bf.file, uint32(len(buf)))
	if err != nil {
		return 0, err
	}
	// Write content:
	n, err := bf.WriteBytes(buf)
	if err != nil {
		return 0, err
	}
	return n + 4, nil
}

func (bf *BorshFile) WriteZSTDByteSlice(buf []byte) (int, error) {
	enc, err := zstdEncoderPool.Get(nil)
	if err != nil {
		return 0, err
	}
	defer zstdEncoderPool.Put(enc)

	encoded := enc.EncodeAll(buf, nil)
	return bf.WriteBorshSlice(encoded)
}

func (bf *BorshFile) ReadUint32LE(buf []byte) (out uint32, err error) {
	return ReadUint32LE(bf.file)
}

func (bf *BorshFile) GetFile() *os.File {
	return bf.file
}

func (bf *BorshFile) ReadBorshSlice() (out []byte, contentLength uint32, err error) {
	return ReadBorshSlice(bf.file)
}

func ReadBorshSlice(reader io.Reader) (out []byte, contentLength uint32, err error) {
	contentLength, err = ReadUint32LE(reader)
	if err != nil {
		return nil, contentLength, err
	}
	contentBuffer := make([]byte, contentLength)
	readLength, err := io.ReadFull(reader, contentBuffer)
	if err != nil {
		return nil, 0, err
	}
	if readLength != int(contentLength) {
		return nil, 0, fmt.Errorf("expected %v bytes, got %v bytes", contentLength, readLength)
	}
	return contentBuffer, contentLength, nil
}

func (bf *BorshFile) ReadZSTDBorshSlice() (contentBuffer []byte, contentLength uint32, err error) {
	return ReadZSTDBorshSlice(bf.file)
}

func ReadZSTDBorshSlice(reader io.Reader) (contentBuffer []byte, contentLength uint32, err error) {
	contentBuffer, contentLength, err = ReadBorshSlice(reader)
	if err != nil {
		return nil, 0, err
	}
	dec, err := zstdDecoderPool.Get(nil)
	if err != nil {
		return nil, 0, err
	}

	decodable, err := dec.DecodeAll(contentBuffer, nil)
	if err != nil {
		return nil, 0, err
	}
	zstdDecoderPool.Put(dec)
	return decodable, contentLength, nil
}

var zstdDecoderPool = zstdpool.NewDecoderPool()
var zstdEncoderPool = zstdpool.NewEncoderPool()

func WriteUint32LE(writer io.Writer, i uint32) (err error) {
	return WriteUint32(writer, i, binary.LittleEndian)
}

func WriteUint32(writer io.Writer, i uint32, order binary.ByteOrder) (err error) {
	buf := make([]byte, 4)
	order.PutUint32(buf, i)
	_, err = writer.Write(buf)
	return err
}

func ReadUint32LE(reader io.Reader) (out uint32, err error) {
	return ReadUint32(reader, binary.LittleEndian)
}

func ReadUint32(reader io.Reader, order binary.ByteOrder) (out uint32, err error) {
	buf := make([]byte, 4)
	n, err := io.ReadFull(reader, buf)
	if err != nil {
		return 0, err
	}
	if n != 4 {
		return 0, fmt.Errorf("expected 4 bytes, got %v", n)
	}
	out = order.Uint32(buf)
	return
}
