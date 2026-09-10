/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util

import (
	"fmt"
	"testing"

	solr "github.com/apache/solr-operator/api/v1beta1"
	"github.com/apache/solr-operator/controllers/util/solr_api"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
)

/*
These tests cover the interaction between the "all replicas of this shard on this node are down"
short-circuit and the maxShardReplicasUnavailable budget in pickPodsToUpdate.

The short-circuit exists so that a pod whose replicas are already down can be restarted without
being counted against the shard's availability budget: taking it down does not reduce the number
of replicas currently serving the shard. That is true, but only as long as some other replica of
the shard is still available. If it is not, then the pod being considered holds the shard's only
replica that is able to come back, and restarting it throws away an in-progress recovery and
extends a full-shard outage instead of shortening it.

This situation is reachable during an ordinary managed rolling update, because every pod that is
taken down forces its peers into leader election and recovery, which is exactly when a replica
briefly reports "down".
*/

const shardAvailabilityPodCount = 6

// shardAvailabilityCloud is a 6 pod cloud using the Managed update strategy with the default
// maxShardReplicasUnavailable of 1.
func shardAvailabilityCloud() *solr.SolrCloud {
	maxShardReplicasUnavailable := intstr.FromInt(1)
	return &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "default"},
		Spec: solr.SolrCloudSpec{
			Replicas: Replicas(shardAvailabilityPodCount),
			SolrAddressability: solr.SolrAddressabilityOptions{
				PodPort: 2000,
			},
			UpdateStrategy: solr.SolrUpdateStrategy{
				Method: solr.ManagedUpdate,
				ManagedUpdateOptions: solr.ManagedUpdateOptions{
					MaxShardReplicasUnavailable: &maxShardReplicasUnavailable,
				},
			},
		},
	}
}

func shardAvailabilityNodeName(podIndex int) string {
	return fmt.Sprintf("foo-solrcloud-%d.foo-solrcloud-headless.default:2000_solr", podIndex)
}

func shardAvailabilityPods(podIndexes ...int) []corev1.Pod {
	pods := make([]corev1.Pod, 0, len(podIndexes))
	for _, podIndex := range podIndexes {
		pods = append(pods, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("foo-solrcloud-%d", podIndex)}})
	}
	return pods
}

/*
shardAvailabilityClusterStatus builds a cluster status for a single shard, col1|shard1, with one
replica on each of the two given pods. Any pod listed in notLive is left out of live_nodes, which
is what a restarting Solr node looks like: its ZooKeeper session is gone, but state.json still
holds whatever state the replica had before the node went away.
*/
func shardAvailabilityClusterStatus(replicaStates map[int]solr_api.SolrReplicaState, notLive map[int]bool) solr_api.SolrClusterStatus {
	liveNodes := make([]string, 0, shardAvailabilityPodCount)
	for podIndex := 0; podIndex < shardAvailabilityPodCount; podIndex++ {
		if !notLive[podIndex] {
			liveNodes = append(liveNodes, shardAvailabilityNodeName(podIndex))
		}
	}

	replicas := make(map[string]solr_api.SolrReplicaStatus, len(replicaStates))
	leaderChosen := false
	for podIndex, state := range replicaStates {
		leader := false
		if !leaderChosen && state == solr_api.ReplicaActive && !notLive[podIndex] {
			leader, leaderChosen = true, true
		}
		replicas[fmt.Sprintf("rep-1-1-%d", podIndex)] = solr_api.SolrReplicaStatus{
			State:    state,
			Core:     "core1",
			NodeName: shardAvailabilityNodeName(podIndex),
			BaseUrl:  fmt.Sprintf("foo-solrcloud-%d.foo-solrcloud-headless.default:2000/solr/rep-1-1-%d", podIndex, podIndex),
			Leader:   leader,
			Type:     solr_api.NRT,
		}
	}

	return solr_api.SolrClusterStatus{
		LiveNodes: liveNodes,
		Collections: map[string]solr_api.SolrCollectionStatus{
			"col1": {
				ConfigName: "conf1",
				Shards: map[string]solr_api.SolrShardStatus{
					"shard1": {
						Replicas: replicas,
						State:    solr_api.ShardActive,
					},
				},
			},
		},
	}
}

func TestPickPodsToUpgradeShardAvailability(t *testing.T) {
	log := ctrl.Log

	// The overseer lives on a pod that holds no replicas and is never out of date, so that the
	// overseer special case does not interfere with these tests.
	overseerLeader := shardAvailabilityNodeName(0)

	// Both replicas of col1|shard1 live on pods 1 and 2, and both pods are out of date.
	outOfDatePods := shardAvailabilityPods(1, 2)
	maxPodsToUpdate := 2

	tests := []struct {
		name          string
		replicaStates map[int]solr_api.SolrReplicaState
		notLive       map[int]bool
		expectedPods  []string
		explanation   string
	}{
		{
			name: "shard has an active replica elsewhere, so the pod with the down replica is taken down",
			replicaStates: map[int]solr_api.SolrReplicaState{
				1: solr_api.ReplicaActive,
				2: solr_api.ReplicaDown,
			},
			notLive:      map[int]bool{},
			expectedPods: []string{"foo-solrcloud-2"},
			explanation: "Pod 2's replica is already down and pod 1 is still serving the shard, so restarting " +
				"pod 2 costs no availability. Pod 1 must wait, because taking it down as well would leave the " +
				"shard with nothing.",
		},
		{
			name: "shard has no available replica, so the pod holding its only recovering replica is protected",
			replicaStates: map[int]solr_api.SolrReplicaState{
				1: solr_api.ReplicaActive,
				2: solr_api.ReplicaDown,
			},
			notLive:      map[int]bool{1: true},
			expectedPods: []string{"foo-solrcloud-1"},
			explanation: "Pod 1 is already gone, so pod 2's down replica is the shard's only way back to being " +
				"served. Restarting pod 2 here would discard that recovery and keep the shard fully unavailable " +
				"for another full pod startup.",
		},
		{
			name: "recovery_failed replicas are still taken down so that the rolling update can make progress",
			replicaStates: map[int]solr_api.SolrReplicaState{
				1: solr_api.ReplicaActive,
				2: solr_api.ReplicaRecoveryFailed,
			},
			notLive:      map[int]bool{1: true},
			expectedPods: []string{"foo-solrcloud-1", "foo-solrcloud-2"},
			explanation: "A recovery_failed replica has given up and will not recover on its own, so protecting " +
				"it would stall the update forever without helping the shard. A restart is the only thing that " +
				"can make it useful again.",
		},
		{
			name: "recovering replicas are protected by the existing availability budget",
			replicaStates: map[int]solr_api.SolrReplicaState{
				1: solr_api.ReplicaActive,
				2: solr_api.ReplicaRecovering,
			},
			notLive:      map[int]bool{1: true},
			expectedPods: []string{"foo-solrcloud-1"},
			explanation: "A recovering replica is not down, so the short-circuit does not apply at all and the " +
				"maxShardReplicasUnavailable budget already refuses pod 2.",
		},
		{
			name: "active replicas are protected by the existing availability budget",
			replicaStates: map[int]solr_api.SolrReplicaState{
				1: solr_api.ReplicaActive,
				2: solr_api.ReplicaActive,
			},
			notLive:      map[int]bool{1: true},
			expectedPods: []string{"foo-solrcloud-1"},
			explanation: "Unchanged behavior: one replica is already unavailable because its node is not live, " +
				"so the second cannot be taken down.",
		},
		{
			name: "both replicas down with no available replica leaves only one pod protected",
			replicaStates: map[int]solr_api.SolrReplicaState{
				1: solr_api.ReplicaDown,
				2: solr_api.ReplicaDown,
			},
			notLive:      map[int]bool{1: true},
			expectedPods: []string{"foo-solrcloud-1"},
			explanation: "Pod 1 is not live and is taken down regardless, but pod 2 still holds the shard's only " +
				"recovering replica and is protected.",
		},
		{
			name: "a shard that is down on live nodes is restarted rather than stalling the update forever",
			replicaStates: map[int]solr_api.SolrReplicaState{
				1: solr_api.ReplicaDown,
				2: solr_api.ReplicaDown,
			},
			notLive:      map[int]bool{},
			expectedPods: []string{"foo-solrcloud-2"},
			explanation: "Every node is live and no replica is recovering, so nothing is going to move these " +
				"replicas out of \"down\" on its own. Refusing both pods would leave the rolling update retrying " +
				"forever with the shard still unavailable, so one pod is taken down. The other is then protected, " +
				"because the pod being taken down is now on its way back and counts as in transition.",
		},
		{
			name: "an orphaned replica on a node this SolrCloud no longer manages does not protect the shard",
			replicaStates: map[int]solr_api.SolrReplicaState{
				1: solr_api.ReplicaDown,
				9: solr_api.ReplicaActive,
			},
			notLive:      map[int]bool{},
			expectedPods: []string{"foo-solrcloud-1", "foo-solrcloud-2"},
			explanation: "Pod 9 is outside this SolrCloud's pod range, so its node is not live and is never coming " +
				"back. Treating it as in transition would protect pod 1 forever, so it is excluded and pod 1 is " +
				"taken down.",
		},
		{
			name: "a recovering replica elsewhere is waited for rather than restarting the down replica",
			replicaStates: map[int]solr_api.SolrReplicaState{
				1: solr_api.ReplicaRecovering,
				2: solr_api.ReplicaDown,
			},
			notLive:      map[int]bool{},
			expectedPods: []string{},
			explanation: "Pod 1's replica is actively recovering and is expected to become active on its own, so " +
				"pod 2 is protected rather than restarted. Pod 1 is held back by the existing availability budget. " +
				"This is a bounded wait on a recovery in progress, not a stall.",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cloud := shardAvailabilityCloud()
			clusterStatus := shardAvailabilityClusterStatus(test.replicaStates, test.notLive)

			podsToUpdate := pickPodsToUpdate(cloud, outOfDatePods, clusterStatus, overseerLeader, maxPodsToUpdate, log)

			assert.ElementsMatch(t, test.expectedPods, getPodNames(podsToUpdate), test.explanation)
			assert.LessOrEqual(t, len(podsToUpdate), maxPodsToUpdate, "The number of pods to update must respect maxPodsUnavailable")
		})
	}
}
