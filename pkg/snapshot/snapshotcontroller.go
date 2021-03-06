package snapshotcontroller

import (
	"fmt"
	"sync"
	"time"

	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	snapshotcontroller "github.com/kubernetes-incubator/external-storage/snapshot/pkg/controller/snapshot-controller"
	snapshotvolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	log "github.com/sirupsen/logrus"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/rest"
)

const (
	snapshotProvisionerName               = "stork-snapshot"
	snapshotProvisionerID                 = "stork"
	provisionerIDAnn                      = "snapshotProvisionerIdentity"
	defaultSyncDuration     time.Duration = 60 * time.Second
)

// SnapshotController Snapshot Controller
type SnapshotController struct {
	Driver      volume.Driver
	lock        sync.Mutex
	started     bool
	stopChannel chan struct{}
}

// GetProvisionerName Gets the name of the provisioner
func GetProvisionerName() string {
	return snapshotProvisionerName
}

// Start Starts the snapshot controller
func (s *SnapshotController) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.started {
		return fmt.Errorf("Extender has already been started")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	clientset, err := k8sutils.GetK8sClient()
	if err != nil {
		return err
	}
	aeclientset, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return err
	}

	snapshotClient, snapshotScheme, err := client.NewClient(config)
	if err != nil {
		return err
	}

	err = client.CreateCRD(aeclientset)
	if err != nil {
		return err
	}

	err = client.WaitForSnapshotResource(snapshotClient)
	if err != nil {
		return err
	}

	plugins := make(map[string]snapshotvolume.Plugin)
	plugins[s.Driver.String()] = s.Driver.GetSnapshotPlugin()

	snapController := snapshotcontroller.NewSnapshotController(snapshotClient, snapshotScheme,
		clientset, &plugins, defaultSyncDuration)

	s.stopChannel = make(chan struct{})

	snapController.Run(s.stopChannel)

	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		log.Fatalf("Error getting server version: %v", err)
	}

	snapProvisioner := newSnapshotProvisioner(clientset, snapshotClient, plugins, snapshotProvisionerID)

	provisioner := controller.NewProvisionController(
		clientset,
		snapshotProvisionerName,
		snapProvisioner,
		serverVersion.GitVersion,
	)
	provisioner.Run(s.stopChannel)

	s.started = true
	return nil
}

// Stop Stops the snapshot controller
func (s *SnapshotController) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.started {
		return fmt.Errorf("Extender has not been started")
	}

	close(s.stopChannel)

	s.started = false
	return nil
}
