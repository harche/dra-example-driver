#!/usr/bin/env bash
# KEP-5517 hugepage overhead reproducer against one node image.
# Usage: run-hp-demo.sh <kind-node-image> <cluster-name>
set -ex
set -o pipefail

IMAGE="$1"
CLUSTER="$2"
WORKER_NODE="${CLUSTER}-worker"
DRIVER_IMAGE="dra-example-driver:overhead-demo"
DRIVER_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

sudo kind delete cluster --name "$CLUSTER" || true
cat > /tmp/hp-kind-config.yaml <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
featureGates:
  DRANodeAllocatableResources: true
nodes:
- role: control-plane
- role: worker
EOF
sudo kind create cluster --name "$CLUSTER" --image "$IMAGE" \
    --config /tmp/hp-kind-config.yaml --wait 2m
sudo kind get kubeconfig --name "$CLUSTER" > ~/kubeconfig-$CLUSTER
export KUBECONFIG=~/kubeconfig-$CLUSTER

sudo kind load docker-image "$DRIVER_IMAGE" --name "$CLUSTER"

helm upgrade -i --create-namespace --namespace dra-example-driver dra-demo \
    "$DRIVER_DIR/deployments/helm/dra-example-driver" \
    --set image.repository="docker.io/library/dra-example-driver" \
    --set image.tag="overhead-demo" \
    --set image.pullPolicy=Never \
    --set overheadDemo.perContainer=1Gi \
    --set overheadDemo.holdPages=2
kubectl rollout status -n dra-example-driver daemonset --timeout=180s

# Wait for overhead-carrying devices.
for _ in $(seq 1 30); do
    n=$(kubectl get resourceslices -o json | jq '[.items[].spec.devices[] | select(.nodeAllocatableResources != null)] | length')
    [ "${n:-0}" -gt 0 ] && break
    sleep 2
done
kubectl get resourceslices -o json | jq -c '[.items[].spec.devices[].nodeAllocatableResources | select(. != null)] | first'
kubectl get node "$WORKER_NODE" -o jsonpath='{.status.allocatable.hugepages-1Gi}'; echo " <- node allocatable hugepages-1Gi"

# Pod 1: GPU claim, init + main reference it; driver holds 2 pages at prepare.
kubectl apply -f "$(dirname "$0")/hp-demo.yaml"
kubectl wait -n hp-demo pod/gpu-user --for=condition=Ready --timeout=180s
echo "---- pod1 claim status ----"
kubectl get pod -n hp-demo gpu-user -o jsonpath='{.status.nodeAllocatableResourceClaimStatuses}' | jq .
echo "---- driver log (holding pages) ----"
kubectl logs -n dra-example-driver -l app.kubernetes.io/component=kubeletplugin --tail=50 | grep -i "holding\|overhead" || true
echo "---- physical hugepage pool on the machine ----"
grep -H "" /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages /sys/kernel/mm/hugepages/hugepages-1048576kB/free_hugepages
echo "---- pod1 pod-level hugetlb cgroup limit ----"
pod_uid=$(kubectl get pod -n hp-demo gpu-user -o jsonpath='{.metadata.uid}' | tr - _)
cg=$(sudo docker exec "$WORKER_NODE" bash -c "find /sys/fs/cgroup -type d -name \"*pod${pod_uid}*\" | head -1")
sudo docker exec "$WORKER_NODE" cat "$cg/hugetlb.1GB.max"

# Pod 2: plain pod wanting 3 real hugepages.
kubectl apply -f "$(dirname "$0")/hp-victim.yaml"
sleep 45
echo "==== RESULT for $IMAGE ===="
kubectl get pod -n hp-demo -o wide
echo "---- pod2 events ----"
kubectl get events -n hp-demo --field-selector involvedObject.name=hugepage-user -o custom-columns=REASON:.reason,MSG:.message | tail -6
echo "---- pod2 logs (if it ran) ----"
kubectl logs -n hp-demo hugepage-user 2>/dev/null | tail -8 || true
kubectl get pod -n hp-demo hugepage-user -o jsonpath='{.status.phase}{" / "}{.status.containerStatuses[0].state}'; echo
echo "==== END RESULT for $IMAGE ===="
