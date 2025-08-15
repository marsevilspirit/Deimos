package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStoreCompression(t *testing.T) {
	// Create test store
	s := newStore()

	// Add some test data
	s.Set("/test1", false, "value1", time.Now().Add(time.Hour))
	s.Set("/test2", false, "value2", time.Now().Add(time.Hour))
	s.Create("/testdir", true, "", false, time.Now().Add(time.Hour))
	s.Set("/testdir/file1", false, "file1value", time.Now().Add(time.Hour))

	// Test normal protobuf serialization
	normalData, err := s.Save()
	assert.NoError(t, err)
	assert.NotNil(t, normalData)

	// Test compressed protobuf serialization
	compressedData, err := s.SaveGzip()
	assert.NoError(t, err)
	assert.NotNil(t, compressedData)

	// Verify compression effect
	t.Logf("Original protobuf size: %d bytes", len(normalData))
	t.Logf("Compressed size: %d bytes", len(compressedData))
	t.Logf("Compression ratio: %.2f%%", float64(len(compressedData))/float64(len(normalData))*100)

	// Compressed data should be smaller than original
	assert.True(t, len(compressedData) < len(normalData))

	// Test recovery from compressed data
	s2 := newStore()
	err = s2.RecoveryGzip(compressedData)
	assert.NoError(t, err)

	// Verify recovered data
	assert.Equal(t, s.CurrentIndex, s2.CurrentIndex)
	assert.Equal(t, s.CurrentVersion, s2.CurrentVersion)

	// Verify specific node data
	event, err := s2.Get("/test1", false, false)
	assert.NoError(t, err)
	assert.Equal(t, "value1", *event.Node.Value)

	event, err = s2.Get("/testdir", true, false)
	assert.NoError(t, err)
	assert.True(t, event.Node.Dir)
}

func TestStoreCompressionWithLargeData(t *testing.T) {
	// Create store with large amount of data
	s := newStore()

	// Add large amount of test data to test compression effect
	for i := 0; i < 1000; i++ {
		path := fmt.Sprintf("/test%d", i)
		value := fmt.Sprintf("value%d", i)
		s.Set(path, false, value, time.Now().Add(time.Hour))
	}

	// Test compression effect
	normalData, err := s.Save()
	assert.NoError(t, err)

	compressedData, err := s.SaveGzip()
	assert.NoError(t, err)

	// Verify compression effect
	t.Logf("Large data - Original protobuf size: %d bytes", len(normalData))
	t.Logf("Large data - Compressed size: %d bytes", len(compressedData))
	t.Logf("Large data - Compression ratio: %.2f%%", float64(len(compressedData))/float64(len(normalData))*100)

	// Compressed data should be smaller than original
	assert.True(t, len(compressedData) < len(normalData))

	// Test recovery
	s2 := newStore()
	err = s2.RecoveryGzip(compressedData)
	assert.NoError(t, err)

	// Verify data integrity after recovery
	assert.Equal(t, s.CurrentIndex, s2.CurrentIndex)

	// Verify several random nodes
	for i := 0; i < 10; i++ {
		path := fmt.Sprintf("/test%d", i*100)
		expectedValue := fmt.Sprintf("value%d", i*100)

		event, err := s2.Get(path, false, false)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, *event.Node.Value)
	}
}

func BenchmarkStoreSaveNormal(b *testing.B) {
	s := newStore()

	// Add some test data
	for i := 0; i < 100; i++ {
		path := fmt.Sprintf("/test%d", i)
		value := fmt.Sprintf("value%d", i)
		s.Set(path, false, value, time.Now().Add(time.Hour))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.Save()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreSaveGzip(b *testing.B) {
	s := newStore()

	// Add some test data
	for i := 0; i < 100; i++ {
		path := fmt.Sprintf("/test%d", i)
		value := fmt.Sprintf("value%d", i)
		s.Set(path, false, value, time.Now().Add(time.Hour))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.SaveGzip()
		if err != nil {
			b.Fatal(err)
		}
	}
}
