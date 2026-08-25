// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package azure

import (
	"context"
	"io/fs"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go/io/gocloud/blobfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

func testADLSBlobFileIO(t *testing.T, ctx context.Context, root string, bucket *blob.Bucket) *blobfs.BlobFileIO {
	t.Helper()

	parsed, err := url.Parse(root)
	require.NoError(t, err)

	bfs, ok := blobfs.New(ctx, bucket, adlsObjectLocationExtractor(parsed)).(*blobfs.BlobFileIO)
	require.True(t, ok)

	return bfs
}

func TestBlobFileIOWalkDirRejectsWrongAzureAuthority(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := testADLSBlobFileIO(t, ctx, "abfs://container@account.dfs.core.windows.net/", bucket)

	err := bfs.WalkDir("abfs://other@account.dfs.core.windows.net/data", func(string, fs.DirEntry, error) error {
		t.Fatal("WalkDir callback should not be called")

		return nil
	})
	require.ErrorContains(t, err, "does not match configured authority")
	require.ErrorIs(t, err, blobfs.ErrUnsupportedObjectAuthority)
}

func TestBlobFileIOWalkDirAzureURI(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	files := []string{
		"path/100%off/file.parquet",
		"path/city=New York/file.parquet",
		"path/to/file.parquet",
	}
	for _, f := range files {
		require.NoError(t, bucket.WriteAll(ctx, f, []byte("data"), nil))
	}

	bfs := testADLSBlobFileIO(t, ctx, "abfs://container@account.dfs.core.windows.net/", bucket)

	var walked []string
	err := bfs.WalkDir("abfs://container@account.dfs.core.windows.net/path", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			walked = append(walked, path)
		}

		return nil
	})
	require.NoError(t, err)

	expected := []string{
		"abfs://container@account.dfs.core.windows.net/path/100%off/file.parquet",
		"abfs://container@account.dfs.core.windows.net/path/city=New York/file.parquet",
		"abfs://container@account.dfs.core.windows.net/path/to/file.parquet",
	}
	assert.ElementsMatch(t, expected, walked)
}
