// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// Package azure registers the Azure Data Lake Storage FileIO backend. Import it
// for its side effects to enable abfs, abfss, wasb and wasbs URIs:
//
//	import _ "github.com/apache/iceberg-go/io/gocloud/azure"
package azure

import (
	"context"
	"net/url"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/io/gocloud/blobfs"
)

// schemes are the URI schemes served by this backend.
var schemes = []string{"abfs", "abfss", "wasb", "wasbs"}

func init() {
	factory := func(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		bucket, err := createAzureBucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}

		return blobfs.New(ctx, bucket, adlsObjectLocationExtractor(parsed)), nil
	}

	for _, scheme := range schemes {
		icebergio.Register(scheme, factory)
	}
}
