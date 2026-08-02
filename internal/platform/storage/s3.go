package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ErrMediaDisabled is returned when media storage was explicitly disabled.
var ErrMediaDisabled = errors.New("media_disabled")

// S3Client wraps the AWS SDK v2 S3 client.
type S3Client struct {
	Client   *s3.Client
	Bucket   string
	disabled bool
}

// NewDisabledS3Client returns a fail-closed storage adapter. It is safe to pass
// through existing media dependencies; every read/write is rejected.
func NewDisabledS3Client() *S3Client {
	return &S3Client{disabled: true}
}

// Enabled reports whether media reads and writes are available.
func (s *S3Client) Enabled() bool {
	return s != nil && !s.disabled && s.Client != nil
}

// NewS3Client creates and configures a new S3Client.
func NewS3Client(endpoint, region, accessKey, secretKey, bucket string, usePathStyle bool) (*S3Client, error) {
	// Configure static credentials provider
	creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("load default config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = usePathStyle
	})

	return &S3Client{
		Client: client,
		Bucket: bucket,
	}, nil
}

// Upload stores bytes in S3.
func (s *S3Client) Upload(ctx context.Context, key string, data []byte, contentType string) error {
	if !s.Enabled() {
		return ErrMediaDisabled
	}
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("s3 put object: %w", err)
	}
	return nil
}

// Download retrieves file stream from S3.
func (s *S3Client) Download(ctx context.Context, key string) (io.ReadCloser, string, error) {
	if !s.Enabled() {
		return nil, "", ErrMediaDisabled
	}
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", err
	}

	contentType := ""
	if out.ContentType != nil {
		contentType = *out.ContentType
	}
	return out.Body, contentType, nil
}
