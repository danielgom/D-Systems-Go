package log

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write      = []byte("hello world")
	width      = uint64(len(write)) + lenWidth
	readWrites = uint64(10)
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)

	defer func(name string) {
		err = os.Remove(name)
		if err != nil {
			log.Fatalln("error while trying to close the test file store", err)
		}
	}(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < readWrites; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < readWrites; i++ {
		read, err := s.Read(pos)
		require.Nil(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.Nil(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.Nil(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}
