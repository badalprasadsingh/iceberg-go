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

// The integration build imports io/gocloud to exercise real buckets, which
// would register the very schemes this file asserts are absent.
//go:build !integration

package hadoop_test

import (
	"testing"

	_ "github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
)

// The Hadoop catalog needs the bucket-backed FileIO type but no cloud backend.
// Importing one here would link all three cloud SDKs into every binary that
// uses this catalog, which is what io/gocloud/blobfs exists to avoid.
func TestImportRegistersNoCloudSchemes(t *testing.T) {
	registered := io.GetRegisteredSchemes()
	for _, scheme := range []string{"s3", "s3a", "s3n", "oss", "gs", "abfs", "abfss", "wasb", "wasbs"} {
		assert.NotContains(t, registered, scheme)
	}
}
