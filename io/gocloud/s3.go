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

package gocloud

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/auth/bearer"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

// ParseAWSConfig parses S3 properties and returns a configuration.
func ParseAWSConfig(ctx context.Context, props map[string]string) (*aws.Config, error) {
	// Remote S3 request signing is not implemented yet.
	if v, ok := props[io.S3RemoteSigningEnabled]; ok {
		if enabled, err := strconv.ParseBool(v); err == nil && enabled {
			return nil, errors.New("remote S3 request signing is not supported")
		}
	}

	opts := []func(*config.LoadOptions) error{}
	var httpClient *awshttp.BuildableClient

	if tok, ok := props["token"]; ok {
		opts = append(opts, config.WithBearerAuthTokenProvider(
			&bearer.StaticTokenProvider{Token: bearer.Token{Value: tok}}))
	}

	if region, ok := props[io.S3Region]; ok {
		opts = append(opts, config.WithRegion(region))
	} else if region, ok := props[io.S3ClientRegion]; ok {
		opts = append(opts, config.WithRegion(region))
	}

	accessKey, secretAccessKey := props[io.S3AccessKeyID], props[io.S3SecretAccessKey]
	token := props[io.S3SessionToken]
	if accessKey != "" || secretAccessKey != "" || token != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			props[io.S3AccessKeyID], props[io.S3SecretAccessKey], props[io.S3SessionToken])))
	}

	if proxy, ok := props[io.S3ProxyURI]; ok {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, fmt.Errorf("invalid s3 proxy url %q: %w", proxy, err)
		}

		httpClient = newS3BuildableClient().WithTransportOptions(
			func(t *http.Transport) {
				t.Proxy = http.ProxyURL(proxyURL)
			},
		)
	}

	if timeout, ok := props[io.S3ConnectTimeout]; ok {
		duration, err := parseS3ConnectTimeout(timeout)
		if err != nil {
			return nil, err
		}

		if httpClient == nil {
			httpClient = newS3BuildableClient()
		}
		httpClient = httpClient.WithDialerOptions(func(d *net.Dialer) {
			d.Timeout = duration
		})
	}

	if httpClient != nil {
		opts = append(opts, config.WithHTTPClient(httpClient))
	}

	awscfg := new(aws.Config)
	var err error
	*awscfg, err = config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return awscfg, nil
}

func parseS3ConnectTimeout(timeout string) (time.Duration, error) {
	var duration time.Duration
	if seconds, err := strconv.ParseFloat(timeout, 64); err == nil {
		duration = time.Duration(seconds * float64(time.Second))
	} else {
		parsedDuration, err := time.ParseDuration(timeout)
		if err != nil {
			return 0, fmt.Errorf("invalid s3.connect-timeout %q: must be seconds as a number or a Go duration string", timeout)
		}
		duration = parsedDuration
	}

	if duration <= 0 {
		return 0, errors.New("s3.connect-timeout must be a positive duration")
	}

	return duration, nil
}

// S3 transport tuning shared by all S3 BuildableClient paths.
const (
	s3MaxIdleConns        = 256
	s3MaxIdleConnsPerHost = 256
	s3MaxConnsPerHost     = 2048
	s3IdleConnTimeout     = 90 * time.Second
)

// newS3BuildableClient returns an AWS buildable HTTP client with the S3
// transport tuning applied. Subsequent WithTransportOptions/WithDialerOptions
// calls preserve this tuning, since the builder clones the transport forward.
func newS3BuildableClient() *awshttp.BuildableClient {
	return awshttp.NewBuildableClient().WithTransportOptions(applyS3TransportTuning)
}

func applyS3TransportTuning(t *http.Transport) {
	t.MaxIdleConns = s3MaxIdleConns
	t.MaxIdleConnsPerHost = s3MaxIdleConnsPerHost
	t.MaxConnsPerHost = s3MaxConnsPerHost
	t.IdleConnTimeout = s3IdleConnTimeout
}

// applyS3ClientOptions returns the s3.Options mutator used when constructing
// the S3 client. It is factored out so the option-resolution logic (custom
// endpoint, path-style addressing, and checksum-calculation mode) can be
// exercised by unit tests without spinning up an S3 client.
func applyS3ClientOptions(endpoint string, props map[string]string) func(*s3.Options) {
	return func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			// AWS SDK Go v2's default RequestChecksumCalculationWhenSupported
			// turns PutObject (and friends) into aws-chunked uploads with a
			// CRC32 trailer. Non-AWS S3-compatible endpoints such as GCS
			// HMAC interop and some MinIO/R2 configurations do not understand
			// the aws-chunked + signed-trailer envelope, so the canonical
			// request the server reconstructs no longer matches the SigV4
			// signature the client sent and the request fails with
			// SignatureDoesNotMatch. Switch to WhenRequired for custom
			// endpoints so the SDK only emits checksum headers/trailers for
			// operations that actually require them, while preserving the
			// default behaviour for genuine AWS S3.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			// gocloud.dev's s3blob writer goes through the S3 transfer
			// manager, which unconditionally populates
			// ChecksumAlgorithm = CRC32 on every PutObject / UploadPart /
			// CreateMultipartUpload input (see resolveChecksumAlgorithm in
			// aws-sdk-go-v2/feature/s3/transfermanager). Once the input
			// carries an explicit algorithm, the SDK's flexible-checksum
			// middleware re-enables aws-chunked + signed trailers even
			// though we set RequestChecksumCalculationWhenRequired above.
			// Strip the field in an Initialize-step middleware so the
			// flexible-checksum middleware (which runs in the Build step)
			// sees an empty algorithm and leaves the body alone.
			o.APIOptions = append(o.APIOptions, stripS3InputChecksumAlgorithm)
			// AWS SDK Go v2 also adds AWS-internal telemetry headers
			// (Amz-Sdk-Invocation-Id, Amz-Sdk-Request) and an explicit
			// Accept-Encoding header to every request, and signs all of
			// them as part of the SigV4 canonical request. GCS's HMAC
			// S3-compatible interop endpoint does not preserve these
			// headers when reconstructing the canonical request on its
			// side, so the server-side signature it derives differs from
			// the one the client sent and the request is rejected with
			// SignatureDoesNotMatch. See aws/aws-sdk-go-v2#1816. Strip
			// them in a Finalize-step middleware that runs before the
			// "Signing" middleware so neither the canonical request the
			// client signs nor the wire request contains them.
			o.APIOptions = append(o.APIOptions, stripGCSIncompatibleSignedHeaders)
		}
		o.UsePathStyle = resolveUsePathStyle(endpoint, props)
		o.DisableLogOutputChecksumValidationSkipped = true
	}
}

// stripS3InputChecksumAlgorithm clears the ChecksumAlgorithm field on the
// PutObject / UploadPart / CreateMultipartUpload inputs that the AWS Go SDK
// (and high-level transfer manager) populate by default.
//
// The SDK's checksum machinery is driven by the AWSChecksum:SetupInputContext
// Initialize middleware, which reads input.ChecksumAlgorithm and stores it on
// the request context. Once that context value is set, downstream Build-step
// middleware unconditionally emits aws-chunked + signed-trailer encoding
// regardless of RequestChecksumCalculationWhenRequired on the client.
//
// Therefore this middleware must be inserted Before AWSChecksum:SetupInputContext
// so it can wipe the algorithm field while SetupInputContext still observes it
// as empty and, combined with WhenRequired, declines to enable chunked encoding.
// Inserting After (or appending at the end of the Initialize step) is too late
// because the algorithm has already been captured.
func stripS3InputChecksumAlgorithm(stack *smithymiddleware.Stack) error {
	m := smithymiddleware.InitializeMiddlewareFunc(
		"iceberg-go/strip-s3-input-checksum-algorithm",
		func(ctx context.Context, in smithymiddleware.InitializeInput, next smithymiddleware.InitializeHandler) (smithymiddleware.InitializeOutput, smithymiddleware.Metadata, error) {
			switch v := in.Parameters.(type) {
			case *s3.PutObjectInput:
				v.ChecksumAlgorithm = ""
			case *s3.UploadPartInput:
				v.ChecksumAlgorithm = ""
			case *s3.CreateMultipartUploadInput:
				v.ChecksumAlgorithm = ""
			}

			return next.HandleInitialize(ctx, in)
		},
	)

	if err := stack.Initialize.Insert(m, "AWSChecksum:SetupInputContext", smithymiddleware.Before); err != nil {
		// Older SDK builds, or a stack that hasn't been fully wired by
		// s3.NewFromConfig yet, may not have the SetupInputContext anchor.
		// Falling back to a Before-of-the-step insert still gives the
		// middleware a chance to run first when the anchor exists at
		// runtime; the explicit Insert above is the load-bearing path.
		return stack.Initialize.Add(m, smithymiddleware.Before)
	}

	return nil
}

// gcsIncompatibleSignedHeaders is the set of HTTP headers that AWS SDK Go v2
// adds to every request and signs, but which GCS's HMAC S3-compatible interop
// endpoint does not preserve when reconstructing the canonical request on the
// server side. Signing them causes SignatureDoesNotMatch on PutObject and the
// other write paths the iceberg-go S3 IO uses.
var gcsIncompatibleSignedHeaders = []string{
	"Amz-Sdk-Invocation-Id",
	"Amz-Sdk-Request",
	"Accept-Encoding",
}

// stripGCSIncompatibleSignedHeaders registers a Finalize-step middleware that
// runs before the SigV4 "Signing" middleware and deletes the headers in
// gcsIncompatibleSignedHeaders from the outbound request, so they end up in
// neither the canonical request the client signs nor the wire request.
func stripGCSIncompatibleSignedHeaders(stack *smithymiddleware.Stack) error {
	m := smithymiddleware.FinalizeMiddlewareFunc(
		"iceberg-go/strip-gcs-incompatible-signed-headers",
		func(ctx context.Context, in smithymiddleware.FinalizeInput, next smithymiddleware.FinalizeHandler) (smithymiddleware.FinalizeOutput, smithymiddleware.Metadata, error) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				for _, h := range gcsIncompatibleSignedHeaders {
					req.Header.Del(h)
				}
			}

			return next.HandleFinalize(ctx, in)
		},
	)

	if err := stack.Finalize.Insert(m, "Signing", smithymiddleware.Before); err != nil {
		// Fall back to adding at the front of the Finalize step so the
		// middleware still runs before any signer that may register later.
		return stack.Finalize.Add(m, smithymiddleware.Before)
	}

	return nil
}

// resolveUsePathStyle determines whether the S3 client should use
// path-style addressing. It defaults to virtual-hosted style for
// standard AWS S3 and path-style for custom endpoints (e.g. MinIO).
// The s3.force-virtual-addressing property can override either default.
func resolveUsePathStyle(endpoint string, props map[string]string) bool {
	usePathStyle := endpoint != ""
	if forceVirtual, ok := props[io.S3ForceVirtualAddressing]; ok {
		if cfgForceVirtual, err := strconv.ParseBool(forceVirtual); err == nil {
			usePathStyle = !cfgForceVirtual
		}
	}

	return usePathStyle
}

func createS3Bucket(ctx context.Context, parsed *url.URL, props map[string]string) (*blob.Bucket, error) {
	var (
		awscfg *aws.Config
		err    error
	)
	if v := utils.GetAwsConfig(ctx); v != nil {
		awscfg = v
	} else {
		awscfg, err = ParseAWSConfig(ctx, props)
		if err != nil {
			return nil, err
		}
	}

	// Default HTTP client when not configured: use the SDK buildable client so
	// proxy, TLS, dial, and HTTP/2 behavior match the usual AWS defaults, with
	// the S3 transport tuning applied (see applyS3TransportTuning).
	if awscfg.HTTPClient == nil {
		awscfg.HTTPClient = newS3BuildableClient()
	}

	endpoint, ok := props[io.S3EndpointURL]
	if !ok {
		endpoint = os.Getenv("AWS_S3_ENDPOINT")
	}

	client := s3.NewFromConfig(*awscfg, applyS3ClientOptions(endpoint, props))

	// Create a *blob.Bucket.
	bucket, err := s3blob.OpenBucketV2(ctx, client, parsed.Host, nil)
	if err != nil {
		return nil, err
	}

	return bucket, nil
}
