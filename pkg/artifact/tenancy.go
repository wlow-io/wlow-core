package artifact

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// SnapshotBucketPrefix is the prefix used for S3/Blob storage buckets containing microVM snapshots.
const SnapshotBucketPrefix = "wlow-snapshots"

// SnapshotEvictionPolicy defines rules for removing old snapshots to free up space.
type SnapshotEvictionPolicy struct {
	MaxBytes int64
	Pins     []string
}

// TenantChunkBucket returns the storage bucket name for a tenant's data chunks.
func TenantChunkBucket(tenant string) string {
	return fmt.Sprintf("wlow-chunks-%s", sanitizeTenant(tenant))
}

// TenantSnapshotBucket returns the storage bucket name for a tenant's microVM snapshots.
func TenantSnapshotBucket(tenant string) string {
	return fmt.Sprintf("%s-%s", SnapshotBucketPrefix, sanitizeTenant(tenant))
}

// EnforceTenantQuota checks if a tenant has exceeded their storage quota.
func (s *Store) EnforceTenantQuota(ctx context.Context, tenant string) error {
	quota, err := s.GetQuota(ctx, tenant)
	if err != nil || quota.MaxChunkBytes <= 0 {
		return err
	}
	used, err := s.tenantChunkBytes(ctx, tenant)
	if err != nil {
		return err
	}
	if used > quota.MaxChunkBytes {
		return errors.New("tenant chunk quota exceeded")
	}
	return nil
}

// EvictSnapshots removes old snapshots based on the provided policy.
func (s *Store) EvictSnapshots(ctx context.Context, tenant string, policy SnapshotEvictionPolicy) (int, error) {
	if policy.MaxBytes <= 0 {
		return 0, nil
	}
	pins := make(map[string]struct{}, len(policy.Pins))
	for _, pin := range policy.Pins {
		pins[pin] = struct{}{}
	}
	prefix := normTenant(tenant) + ".snapshot."
	keys, err := s.manifests.Keys(ctx)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for idx := 0; idx < len(keys) && idx < maxSnapshotEvictions; idx++ {
		if !strings.HasPrefix(keys[idx], prefix) {
			continue
		}
		if _, ok := pins[keys[idx]]; ok {
			continue
		}
		if err := s.manifests.Delete(ctx, keys[idx]); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func sanitizeTenant(tenant string) string {
	tenant = normTenant(tenant)
	if safeName.MatchString(tenant) {
		return tenant
	}
	return DefaultTenant
}

const maxSnapshotEvictions = 1 << 20
