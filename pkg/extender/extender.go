package extender

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storklog "github.com/libopenstorage/stork/pkg/log"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
)

const (
	filter     = "filter"
	prioritize = "prioritize"
	// priorityScore Score by which each node is bumped if it has data for a volume
	priorityScore = 100
	// defaultScore Score assigned to a node which doesn't have data for any volume
	defaultScore = 10

	storkInitializerName = "stork.initializer.kubernetes.io"
	storkSchedulerName   = "stork"
)

// Extender Scheduler extender
type Extender struct {
	Driver      volume.Driver
	server      *http.Server
	lock        sync.Mutex
	started     bool
	stopChannel chan struct{}
}

// Start Starts the extender
func (e *Extender) Start() error {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.started {
		return fmt.Errorf("Extender has already been started")
	}

	// TODO: Make the listen port configurable
	e.server = &http.Server{Addr: ":8099"}

	http.HandleFunc("/", e.serveHTTP)
	go func() {
		if err := e.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Panicf("Error starting extender server: %v", err)
		}
	}()

	config, err := rest.InClusterConfig()
	if err != nil {
		log.Errorf("Error getting cluster config: %v", err)
		return err
	}

	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Errorf("Error getting client, %v", err)
		return err
	}

	restClient := k8sClient.CoreV1().RESTClient()
	podsWatchlist := cache.NewListWatchFromClient(restClient, "pods", v1.NamespaceAll, fields.Everything())

	includeUninitializedWatchlist := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.IncludeUninitialized = true
			return podsWatchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.IncludeUninitialized = true
			return podsWatchlist.Watch(options)
		},
	}

	resyncPeriod := 30 * time.Second

	_, initController := cache.NewInformer(includeUninitializedWatchlist, &v1.Pod{}, resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				err := e.initializeApp(obj, k8sClient)
				if err != nil {
					logrus.Errorf("Error initializing pod: %v", err)
				}
			},
		},
	)

	e.stopChannel = make(chan struct{})
	go initController.Run(e.stopChannel)
	e.started = true
	return nil
}

// Stop Stops the extender
func (e *Extender) Stop() error {
	e.lock.Lock()
	defer e.lock.Unlock()

	if !e.started {
		return fmt.Errorf("Extender has not been started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.server.Shutdown(ctx); err != nil {
		return err
	}
	close(e.stopChannel)
	e.started = false
	return nil
}

func (e *Extender) serveHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.Contains(req.URL.Path, filter) {
		e.processFilterRequest(w, req)
	} else if strings.Contains(req.URL.Path, prioritize) {
		e.processPrioritizeRequest(w, req)
	} else {
		http.Error(w, "Unsupported request", http.StatusNotFound)
	}
}

// The driver might not return fully qualified hostnames, so check if the short
// hostname matches too
func (e *Extender) isHostnameMatch(driverHostname string, k8sHostname string) bool {
	if driverHostname == k8sHostname {
		return true
	}
	if strings.HasPrefix(k8sHostname, driverHostname+".") {
		return true
	}
	return false
}

func (e *Extender) processFilterRequest(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	defer func() {
		if err := req.Body.Close(); err != nil {
			log.Warnf("Error closing decoder")
		}
	}()
	encoder := json.NewEncoder(w)

	var args schedulerapi.ExtenderArgs
	if err := decoder.Decode(&args); err != nil {
		log.Errorf("Error decoding filter request: %v", err)
		http.Error(w, "Decode error", http.StatusBadRequest)
		return
	}

	pod := &args.Pod
	storklog.PodLog(pod).Debugf("Nodes in filter request:")
	for _, node := range args.Nodes.Items {
		storklog.PodLog(pod).Debugf("%+v", node.Status.Addresses)
	}

	filteredNodes := []v1.Node{}
	driverVolumes, err := e.Driver.GetPodVolumes(pod)

	if err != nil {
		storklog.PodLog(pod).Warnf("Error getting volumes for Pod for driver: %v", err)
		if _, ok := err.(*volume.ErrPVCPending); ok {
			http.Error(w, "Waiting for PVC to be bound", http.StatusBadRequest)
			return
		}
	} else if len(driverVolumes) > 0 {
		driverNodes, err := e.Driver.GetNodes()
		if err != nil {
			storklog.PodLog(pod).Errorf("Error getting list of driver nodes, returning all nodes")
		} else {
			for _, node := range args.Nodes.Items {
				for _, address := range node.Status.Addresses {
					if address.Type != v1.NodeHostName {
						continue
					}

					for _, driverNode := range driverNodes {
						if e.isHostnameMatch(driverNode.Hostname, address.Address) &&
							driverNode.Status == volume.NodeOnline {
							filteredNodes = append(filteredNodes, node)
						}
					}
				}
			}
		}
	}

	// If we filtered out all the nodes, or didn't find a PVC that
	// interested us, return all the nodes from the request
	if len(filteredNodes) == 0 {
		filteredNodes = args.Nodes.Items
	}

	storklog.PodLog(pod).Debugf("Nodes in filter response:")
	for _, node := range filteredNodes {
		log.Debugf("%+v", node.Status.Addresses)
	}
	response := &schedulerapi.ExtenderFilterResult{
		Nodes: &v1.NodeList{
			Items: filteredNodes,
		},
	}
	if err := encoder.Encode(response); err != nil {
		storklog.PodLog(pod).Fatalf("Error encoding filter response: %+v : %v", response, err)
	}
}

func (e *Extender) processPrioritizeRequest(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	defer func() {
		if err := req.Body.Close(); err != nil {
			log.Warnf("Error closing decoder")
		}
	}()
	encoder := json.NewEncoder(w)

	var args schedulerapi.ExtenderArgs
	if err := decoder.Decode(&args); err != nil {
		log.Errorf("Error decoding prioritize request: %v", err)
		http.Error(w, "Decode error", http.StatusBadRequest)
		return
	}

	pod := &args.Pod
	storklog.PodLog(pod).Debugf("Nodes in prioritize request:")
	for _, node := range args.Nodes.Items {
		storklog.PodLog(pod).Debugf("%+v", node.Status.Addresses)
	}
	respList := schedulerapi.HostPriorityList{}
	driverVolumes, err := e.Driver.GetPodVolumes(pod)
	driverNodes, err := e.Driver.GetNodes()

	// Create a map for ID->Hostname
	idMap := make(map[string]string)
	for _, node := range driverNodes {
		idMap[node.ID] = node.Hostname
	}

	priorityMap := make(map[string]int)
	if err != nil {
		storklog.PodLog(pod).Warnf("Error getting volumes for Pod for driver: %v", err)
		if _, ok := err.(*volume.ErrPVCPending); ok {
			http.Error(w, "Waiting for PVC to be bound", http.StatusBadRequest)
			return
		}
	} else if len(driverVolumes) > 0 {
		for _, volume := range driverVolumes {
			storklog.PodLog(pod).Debugf("Volume allocated on nodes:")
			for _, node := range volume.DataNodes {
				log.Debugf("%+v", node)
			}

			for _, datanode := range volume.DataNodes {
				for _, node := range args.Nodes.Items {
					for _, address := range node.Status.Addresses {
						if address.Type != v1.NodeHostName {
							continue
						}

						if e.isHostnameMatch(idMap[datanode], address.Address) {
							// Increment score for every volume that is required by
							// the pod and is present on the node
							_, ok := priorityMap[node.Name]
							if !ok {
								priorityMap[node.Name] = priorityScore
							} else {
								priorityMap[node.Name] += priorityScore
							}
						}
					}
				}
			}
		}
	}

	// For any nodes that didn't have any volumes, assign it a
	// default score so that it doesn't get completelt ignored
	// by the scheduler
	for _, node := range args.Nodes.Items {
		score, ok := priorityMap[node.Name]
		if !ok {
			score = defaultScore
		}
		hostPriority := schedulerapi.HostPriority{Host: node.Name, Score: score}
		respList = append(respList, hostPriority)
	}

	storklog.PodLog(pod).Debugf("Nodes in response:")
	for _, node := range respList {
		storklog.PodLog(pod).Debugf("%+v", node)
	}

	if err := encoder.Encode(respList); err != nil {
		storklog.PodLog(pod).Fatalf("Failed to encode response: %v", err)
	}
	return
}

func (e *Extender) initializePod(pod *v1.Pod, clientset *kubernetes.Clientset) error {
	if pod.ObjectMeta.GetInitializers() == nil {
		return nil
	}

	pendingInitializers := pod.ObjectMeta.GetInitializers().Pending
	if storkInitializerName != pendingInitializers[0].Name {
		return nil
	}

	driverVolumes, err := e.Driver.GetPodVolumes(pod)
	if err != nil {
		if _, ok := err.(*volume.ErrPVCPending); !ok {
			storklog.PodLog(pod).Infof("Error getting volumes for pod: %v", err)
			return err
		}
	} else if len(driverVolumes) == 0 {
		return nil
	}

	oldData, err := json.Marshal(pod)
	if err != nil {
		return err
	}

	o, err := runtime.NewScheme().DeepCopy(pod)
	if err != nil {
		return err
	}
	updatedPod := o.(*v1.Pod)

	if len(pendingInitializers) == 1 {
		updatedPod.ObjectMeta.Initializers.Pending = nil
	} else if len(pendingInitializers) > 1 {
		updatedPod.ObjectMeta.Initializers.Pending = append(pendingInitializers[:0], pendingInitializers[1:]...)
	}

	updatedPod.Spec.SchedulerName = "stork"
	newData, err := json.Marshal(updatedPod)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Pod{})
	if err != nil {
		return err
	}

	_, err = clientset.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}

func (e *Extender) initializeApp(obj interface{}, clientset *kubernetes.Clientset) error {
	if pod, ok := obj.(*v1.Pod); ok {
		return e.initializePod(pod, clientset)
	}
	return fmt.Errorf("invalid app type: %v", obj)
}
