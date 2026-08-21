# KEP-5517 overhead lifetime reproducer

This demo shows what happens when the peak based init container accounting
introduced by [kubernetes/kubernetes#141375](https://github.com/kubernetes/kubernetes/pull/141375)
meets a DRA driver whose declared `PerContainer` overhead is allocated at
`NodePrepareResources` and released only at `NodeUnprepareResources`, which
are the only lifecycle events the DRA kubelet plugin API offers. Such a
driver holds an init container's overhead share for the entire life of the
pod, while the peak model stops reserving it once the init phase is over.

The driver patch on this branch makes dra-example-driver an honest driver of
exactly that kind:

* `overheadDemo.perContainer=1Gi` publishes `hugepages-1Gi` overhead of 1Gi
  per container reference on every device
  (`internal/profiles/gpu/gpu.go`).
* `overheadDemo.holdPages=2` makes the kubelet plugin physically allocate
  what it declares when a claim is prepared: 2 x 1Gi hugepages held in a
  hugetlbfs file, freed when the claim is unprepared at pod termination
  (`cmd/dra-example-kubeletplugin/driver.go`, `cmd/hugehold/main.go`).

## Scenario

Node with a pool of 4 x 1Gi hugepages.

* Pod 1 (`hp-demo.yaml`): an init container and a main container both
  reference a claim for one GPU. The main container also requests and maps
  1 x 1Gi hugepage of its own. While it runs, physical usage is
  2 pages held by the driver plus 1 page mapped by the worker, so 3 of 4.
* Pod 2 (`hp-victim.yaml`): a plain pod, no DRA, requesting and mapping
  2 x 1Gi hugepages.

Ledger for pod 1:

* sum model (v1.37.0-rc.0): `max-spec(1Gi) + 2 x 1Gi overhead = 3Gi`
* peak model (PR 141375): `max(0 + 1Gi, 1Gi) + 1Gi pod lifetime = 2Gi`

The physical truth is the sum. The peak model under counts by exactly the
init container's share because the driver has no event on which it could
have released that share.

## Observed results

Same driver, same manifests, only the node image differs.

| | v1.37.0-rc.0 (sum) | PR #141375 head f42fb4b (peak) |
|---|---|---|
| pod 1 | Running | Running |
| driver log | `holding 2 x 1Gi hugepages in /dev/hugepages-demo-1g/<claim-uid>` | same |
| node `free_hugepages` before pod 2 | 2 (1 free + 1 reserved by worker) | same |
| pod 2 scheduling | `FailedScheduling: Insufficient hugepages-1Gi` | `Successfully assigned` |
| pod 2 outcome | Pending, correctly refused | started, `OSError: [Errno 12] Out of memory`, Error |

The sum ledger refuses the placement because the memory genuinely is not
there. The peak ledger schedules the pod onto pages the driver still holds,
and the pod crashes at mmap time. With regular memory instead of hugepages
the same gap shows up as pod or node level OOM instead of a clean ENOMEM.

Note that the divergence only appears when the main phase footprint is at
least the init container's overhead share; with nothing else in the pod the
two models produce the same number, since the pod lifetime part of the
overhead is added after the phase max.

## Running it

Requirements: a Linux host with 4 free 1Gi hugepages
(`echo 4 > /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages`),
docker, kind, helm, kubectl, jq.

```sh
docker build -f deployments/container/Dockerfile \
    --build-arg GO_VERSION=1.26.0 --build-arg BASE_IMAGE=docker.io/ubuntu:22.04 \
    -t dra-example-driver:overhead-demo .

# Build node images: kind build node-image v1.37.0-rc.0 ... and one from the
# PR branch, then:
./demo/overhead-lifetime/run.sh kindest/node:v1.37.0-rc.0 hpbase
./demo/overhead-lifetime/run.sh kindest/node:pr141375     hppr
```

The script prints the published overhead, the pod 1 claim status, the driver
holding log, the physical pool counters, the pod level hugetlb cgroup limit,
and the scheduling outcome for pod 2.
