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

package gocloud_test

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/io/gocloud/blobfs"
	"github.com/apache/iceberg-go/io/gocloud/gcs"
	"github.com/apache/iceberg-go/io/gocloud/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// For callers who still have not migrated
var (
	_ *blobfs.FileIO      = (*gocloud.BlobFileIO)(nil)
	_ blobfs.KeyExtractor = gocloud.KeyExtractor(nil)
	_ error               = gocloud.ErrEmptyObjectKey
	_ error               = gocloud.ErrUnsupportedObjectAuthority
)

func TestDeprecatedParseConfigWrappers(t *testing.T) {
	ctx := context.Background()

	t.Run("ParseGCSConfig", func(t *testing.T) {
		for _, props := range []map[string]string{
			{},
			{io.GCSUseJSONAPI: "true"},
			{io.GCSUseJSONAPI: "false"},
			{io.GCSUseJSONAPI: "not-a-bool"},
			{io.GCSEndpoint: "http://localhost:4443"},
			{io.GCSEndpoint: "http://localhost:4443", io.GCSUseJSONAPI: "true"},
		} {
			want, got := gcs.ParseGCSConfig(props), gocloud.ParseGCSConfig(props)
			assert.Len(t, got.ClientOptions, len(want.ClientOptions))
			assert.Equal(t, want, got, "props: %v", props)
		}
	})

	t.Run("ParseAWSConfig", func(t *testing.T) {
		for _, props := range []map[string]string{
			{},
			{io.S3Region: "us-west-2"},
			{io.S3ClientRegion: "eu-central-1"},
			{io.S3Region: "us-west-2", io.S3ClientRegion: "eu-central-1"},
			{io.S3AccessKeyID: "ak", io.S3SecretAccessKey: "sk"},
			{io.S3AccessKeyID: "ak", io.S3SecretAccessKey: "sk", io.S3SessionToken: "st"},
			{"token": "bearer-token"},
		} {
			want, wantErr := s3.ParseAWSConfig(ctx, props)
			got, gotErr := gocloud.ParseAWSConfig(ctx, props)

			require.NoError(t, wantErr, "props: %v", props)
			require.NoError(t, gotErr, "props: %v", props)
			assert.Equal(t, want.Region, got.Region, "props: %v", props)

			wantCreds, err := want.Credentials.Retrieve(ctx)
			require.NoError(t, err)
			gotCreds, err := got.Credentials.Retrieve(ctx)
			require.NoError(t, err)
			assert.Equal(t, wantCreds.AccessKeyID, gotCreds.AccessKeyID, "props: %v", props)
			assert.Equal(t, wantCreds.SessionToken, gotCreds.SessionToken, "props: %v", props)
		}
	})

	// An error from the backend must surface through the wrapper unchanged.
	t.Run("error is relayed", func(t *testing.T) {
		props := map[string]string{io.S3RemoteSigningEnabled: "true"}
		_, wantErr := s3.ParseAWSConfig(ctx, props)
		_, gotErr := gocloud.ParseAWSConfig(ctx, props)
		require.Error(t, wantErr)
		require.Error(t, gotErr)
		assert.Equal(t, wantErr.Error(), gotErr.Error())
	})
}
