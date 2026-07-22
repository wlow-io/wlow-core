package artifact

import "strings"

const (
	// SubjectManifestPut is the NATS subject for uploading a manifest.
	SubjectManifestPut = "wlow.manifests.put"
	// SubjectTenantKey is the NATS subject for retrieving a tenant key.
	SubjectTenantKey = "wlow.tenants.key"
)

// DescriptorRefKeyPart converts an OCI digest to a NATS KV key segment.
func DescriptorRefKeyPart(digest string) string {
	return "oci." + strings.ReplaceAll(digest, ":", ".")
}

// ErrorResponse is a standard error reply for artifact protocol requests.
type ErrorResponse struct {
	Error string `json:"error"`
}

// ManifestPutRequest is the request payload for uploading a manifest.
type ManifestPutRequest struct {
	Manifest Manifest `json:"manifest"`
	Tags     []string `json:"tags,omitempty"`
}

// ManifestPutResponse is the response payload for a successful manifest upload.
type ManifestPutResponse struct {
	ProcessorID  string `json:"processor_id"`
	Version      string `json:"version"`
	ArtifactHash string `json:"artifact_hash"`
}

// TenantKeyRequest is the request payload for retrieving a tenant key.
type TenantKeyRequest struct {
	Tenant string `json:"tenant,omitempty"`
}

// TenantKeyResponse is the response payload containing a tenant key.
type TenantKeyResponse struct {
	Tenant string `json:"tenant"`
	Key    []byte `json:"key"`
}
