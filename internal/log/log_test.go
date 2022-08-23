package log

import (
	api "github.com/danielgom/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRange,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			require.NoError(t, err)
			defer func() {
				err = os.RemoveAll(dir)
				if err != nil {
					return
				}
			}()
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	ap := &api.Record{Value: []byte("hello world")}
	off, err := log.append(ap)

	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.read(off)
	require.NoError(t, err)
	require.Equal(t, ap.Value, read.Value)

}

func testOutOfRange(t *testing.T, log *Log) {
	read, err := log.read(1)
	require.Nil(t, read)
	apiErr := err.(*api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, log *Log) {
	record := &api.Record{Value: []byte("hello world")}

	for idx := 0; idx < 5; idx++ {
		_, err := log.append(record)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	offset := log.LowestOffset()
	require.Equal(t, uint64(0), offset)
	off, err := log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	newLog, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off = newLog.LowestOffset()
	require.Equal(t, uint64(0), off)
	off, err = newLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	record := &api.Record{Value: []byte("hello world")}

	offset, err := log.append(record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	reader := log.Reader()
	readRecord, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(readRecord[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, record.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	record := &api.Record{Value: []byte("hello world")}

	for idx := 0; idx < 3; idx++ {
		_, err := log.append(record)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.read(0)
	require.Error(t, err)

}
