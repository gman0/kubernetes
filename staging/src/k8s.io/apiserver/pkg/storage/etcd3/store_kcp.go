/*
Copyright 2022 The KCP Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package etcd3

import (
	"strings"

	"github.com/kcp-dev/logicalcluster/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"
)

func stripResourceOriginFromWildcardKey(keyWithoutPrefix string, crdRequest, partialMetadataRequest bool) string {
	// Relationship between key prefix and key:
	//
	// When crdRequest=false:
	//
	//    Prefix when:                <Storage prefix> / <Group> / <Resource> / [ <Shard> / ] <Cluster> / [ <Namespace> / ] <Name>
	//                                ^                                       ^           ^             ^
	//       both shard and cluster   |                                       |           |             |
	//       are wildcards:           +---------------------------------------+           |             |
	//                                |                                                   |             |
	//       only cluster is          |                                                   |             |
	//       wildcard:                +---------------------------------------------------+             |
	//                                |                                                                 |
	//       none are wildcards:      +-----------------------------------------------------------------+
	//
	// When crdRequest=true:
	//
	//    CRD-based resources contain additional segment, <Identity or "customresources">.
	//    This segment is NOT part of the prefix if partialMetadataRequest=true so that
	//    resources of the same GR can be matched regardless of their identity or CRD origin.
	//
	//    Prefix when:                      <Storage prefix> / <Group> / <Resource> / <Identity or "customresources"> / [ <Shard> / ] <Cluster> / [ <Namespace> / ] <Name>
	//                                      ^                                       ^                                 ^
	//       partialMetadataRequest=true,   |                                       |                                 |
	//       shard&cluster are wildcards:   +---------------------------------------+                                 |
	//                                      |                                                                         |
	//       partialMetadataRequest=false,  |                                                                         |
	//       shard&cluster are wildcards:   +-------------------------------------------------------------------------+
	//
	//       N.B. non-wildcard cases with partialMetadataRequest=false
	//       behave the same as with crdRequest=false.
	//
	// In case of `partialMetadataRequest==true && crdRequest==true`,
	// the <Identity or "customresources"> segment is still part
	// of the keyWithoutPrefix. It must be stripped, so that shard
	// and cluster names can be parsed out correctly.

	if !crdRequest || !partialMetadataRequest {
		// Don't need to do anything: the prefix already contains the origin,
		// and that already has been stripped off in keyWithoutPrefix.
		return keyWithoutPrefix
	}

	// We need to drop the first segment (the <Identity or "customresources">) off the keyWithoutPrefix.

	segmentStart := strings.IndexByte(keyWithoutPrefix, '/')
	if segmentStart < 0 {
		return keyWithoutPrefix
	}
	if segmentStart == len(keyWithoutPrefix) {
		return keyWithoutPrefix
	}
	return keyWithoutPrefix[segmentStart+1:]
}

// adjustClusterNameIfWildcard determines the logical cluster name. If this is not a cluster-wildcard list/watch request,
// the cluster name is returned unmodified. Otherwise, the cluster name is extracted from the storage key.
func adjustClusterNameIfWildcard(shard genericapirequest.Shard, cluster *genericapirequest.Cluster, crdRequest bool, keyPrefix, key string) logicalcluster.Name {
	if !cluster.Wildcard {
		return cluster.Name
	}

	keyWithoutPrefix := strings.TrimPrefix(key, keyPrefix)
	keyWithoutOrigin := stripResourceOriginFromWildcardKey(keyWithoutPrefix, crdRequest, cluster.PartialMetadataRequest)

	// The remaining key is in format:
	//   [ <Shard> ] / <Cluster> / <Remainder...>
	parts := strings.SplitN(keyWithoutOrigin, "/", 3)

	extract := func(minLen, i int) logicalcluster.Name {
		if len(parts) < minLen {
			klog.Warningf("shard=%s cluster=%v invalid key=%s had %d parts, wanted %d", shard, cluster, keyWithoutOrigin, len(parts), minLen)
			return ""
		}
		return logicalcluster.Name(parts[i])
	}

	if shard.Empty() {
		// It's only <Cluster> / <Remainder...>
		return extract(2, 0)
	}
	// It's <Shard> / <Cluster> / <Remainder...>
	return extract(3, 1)
}

// adjustShardNameIfWildcard determines a shard name. If this is not a shard-wildcard request,
// the shard name is returned unmodified. Otherwise, the shard name is extracted from the storage key.
func adjustShardNameIfWildcard(shard genericapirequest.Shard, cluster *genericapirequest.Cluster, crdRequest bool, keyPrefix, key string) genericapirequest.Shard {
	if !shard.Empty() && !shard.Wildcard() {
		return shard
	}

	if !shard.Wildcard() {
		// no-op: we can only assign shard names
		// to a request that explicitly asked for it
		return ""
	}

	keyWithoutPrefix := strings.TrimPrefix(key, keyPrefix)
	keyWithoutOrigin := stripResourceOriginFromWildcardKey(keyWithoutPrefix, crdRequest, cluster.PartialMetadataRequest)

	// The remaining key is in format:
	//   <Shard> / <Cluster> / <Remainder...>
	parts := strings.SplitN(keyWithoutOrigin, "/", 3)
	if len(parts) < 3 {
		klog.Warningf("unable to extract a shard name, invalid key=%s had %d parts, wanted %d", keyWithoutOrigin, len(parts), 3)
		return ""
	}
	return genericapirequest.Shard(parts[0])
}

// annotateDecodedObjectWith applies clusterName and shardName to an object.
// This is necessary because we don't store the cluster name and the shard name in the objects in storage.
// Instead, they are derived from the storage key, and then applied after retrieving the object from storage.
func annotateDecodedObjectWith(obj interface{}, clusterName logicalcluster.Name, shardName genericapirequest.Shard) {
	var s nameSetter

	switch t := obj.(type) {
	case metav1.ObjectMetaAccessor:
		s = t.GetObjectMeta()
	case nameSetter:
		s = t
	default:
		klog.Warningf("Could not set ClusterName %s, ShardName %s on object: %T", clusterName, shardName, obj)
		return
	}

	annotations := s.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[logicalcluster.AnnotationKey] = clusterName.String()
	if !shardName.Empty() {
		annotations[genericapirequest.ShardAnnotationKey] = shardName.String()
	}
	s.SetAnnotations(annotations)
}

type nameSetter interface {
	GetAnnotations() map[string]string
	SetAnnotations(a map[string]string)
}
