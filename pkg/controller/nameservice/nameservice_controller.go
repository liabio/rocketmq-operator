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

// Package nameservice contains the implementation of the NameService CRD reconcile function
package nameservice

import (
	"context"
	"fmt"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"time"

	rocketmqv1alpha1 "github.com/apache/rocketmq-operator/pkg/apis/rocketmq/v1alpha1"
	cons "github.com/apache/rocketmq-operator/pkg/constants"
	"github.com/apache/rocketmq-operator/pkg/share"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_nameservice")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new NameService Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileNameService{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("nameservice-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource NameService
	err = c.Watch(&source.Kind{Type: &rocketmqv1alpha1.NameService{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner NameService
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &rocketmqv1alpha1.NameService{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileNameService implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileNameService{}

// ReconcileNameService reconciles a NameService object
type ReconcileNameService struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a NameService object and makes changes based on the state read
// and what is in the NameService.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileNameService) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling NameService")

	// Fetch the NameService instance
	instance := &rocketmqv1alpha1.NameService{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Check if the statefulSet already exists, if not create a new one
	found := &appsv1.StatefulSet{}

	dep := r.statefulSetForNameService(instance)
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: dep.Name, Namespace: dep.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {
		err = r.client.Create(context.TODO(), dep)
		if err != nil {
			reqLogger.Error(err, "Failed to create new StatefulSet of NameService", "StatefulSet.Namespace", dep.Namespace, "StatefulSet.Name", dep.Name)
		}
		// StatefulSet created successfully - return and requeue
		return reconcile.Result{Requeue: true}, nil
	} else if err != nil {
		reqLogger.Error(err, "Failed to get NameService StatefulSet.")
	}

	// Define a new service object
	svc := r.newHeadlessService(instance)

	// Check if the service already exists, if not create a new one
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, svc)
	if err != nil && errors.IsNotFound(err) {
		// Create service
		if err := r.client.Create(context.TODO(), svc); err != nil {
			reqLogger.Error(err, "Failed to create new Service of NameService", "Service.Namespace", svc.Namespace, "Service.Name", svc.Name)
			return reconcile.Result{}, err
		}
		// Service created successfully - return and requeue
		return reconcile.Result{Requeue: true}, nil
	} else if err != nil {
		reqLogger.Error(err, "Failed to get NameService Service.")
		return reconcile.Result{}, err
	}

	exporter := &appsv1.StatefulSet{}
	exporterName := instance.Name + "-exporter"
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: exporterName, Namespace: instance.Namespace}, exporter)
	if err != nil && errors.IsNotFound(err) {
		// create if not exist
		exporter = r.ensureExporterStatefulSet(instance)
		err = r.client.Create(context.TODO(), exporter)
		if err != nil {
			reqLogger.Error(err, "Failed to create new StatefulSet of Exporter", "StatefulSet.Namespace", instance.Namespace, "StatefulSet.Name", exporterName)
		}
		// StatefulSet created successfully - return and requeue
		return reconcile.Result{Requeue: true}, nil
	} else if err != nil {
		reqLogger.Error(err, "Failed to get NameService StatefulSet.")
	} else {
		// update if exist
		exporter = r.ensureExporterStatefulSet(instance)
		err = r.client.Update(context.TODO(), exporter)
		if err != nil {
			reqLogger.Error(err, "Failed to create new StatefulSet of Exporter", "StatefulSet.Namespace", instance.Namespace, "StatefulSet.Name", exporterName)
		}
	}

	// Ensure the statefulSet size is the same as the spec
	size := instance.Spec.Size
	if *found.Spec.Replicas != size {
		found.Spec.Replicas = &size
		err = r.client.Update(context.TODO(), found)
		reqLogger.Info("NameService Updated")
		if err != nil {
			reqLogger.Error(err, "Failed to update StatefulSet.", "StatefulSet.Namespace", found.Namespace, "StatefulSet.Name", found.Name)
			return reconcile.Result{}, err
		}
	}

	return r.updateNameServiceStatus(instance, request, true)
}

func (r *ReconcileNameService) updateNameServiceStatus(instance *rocketmqv1alpha1.NameService, request reconcile.Request, requeue bool) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Check the NameServers status")
	// List the pods for this nameService's statefulSet
	podList := &corev1.PodList{}
	labelSelector := labels.SelectorFromSet(labelsForNameService(instance.Name))
	listOps := &client.ListOptions{
		Namespace:     instance.Namespace,
		LabelSelector: labelSelector,
	}
	err := r.client.List(context.TODO(), listOps, podList)
	if err != nil {
		reqLogger.Error(err, "Failed to list pods.", "NameService.Namespace", instance.Namespace, "NameService.Name", instance.Name)
		return reconcile.Result{Requeue: true}, err
	}
	// Generate service domain
	serverNames := getNameServers(instance.Name, instance.Namespace, instance.Spec.K8sClusterDomain, instance.Spec.Size)
	// Update status.NameServers if needed
	if !reflect.DeepEqual(serverNames, instance.Status.NameServers) {
		/*oldNameServerListStr := ""
		for _, value := range instance.Status.NameServers {
			oldNameServerListStr = oldNameServerListStr + value + ":9876;"
		}

		nameServerListStr := ""
		for _, value := range hostIps {
			nameServerListStr = nameServerListStr + value + ":9876;"
		}
		share.NameServersStr = nameServerListStr[:len(nameServerListStr)-1]
		reqLogger.Info("share.NameServersStr:" + share.NameServersStr)

		if len(oldNameServerListStr) <= cons.MinIpListLength {
			oldNameServerListStr = share.NameServersStr
		} else if len(share.NameServersStr) > cons.MinIpListLength {
			oldNameServerListStr = oldNameServerListStr[:len(oldNameServerListStr)-1]
			share.IsNameServersStrUpdated = true
		}
		reqLogger.Info("oldNameServerListStr:" + oldNameServerListStr)

		instance.Status.NameServers = hostIps*/

		// build new nameServerListStr
		nameServerListStr := ""
		for _, value := range serverNames {
			var builder strings.Builder
			builder.WriteString(nameServerListStr)
			builder.WriteString(value)
			builder.WriteString(":9876;")
			nameServerListStr = builder.String()
		}
		if nameServerListStr == "" {
			share.NameServersStr = nameServerListStr
		} else {
			share.NameServersStr = nameServerListStr[:len(nameServerListStr)-1]
		}
		reqLogger.Info("share.NameServersStr:" + share.NameServersStr)

		// build old NameServerListStr
		oldNameServerListStr := ""
		for _, value := range instance.Status.NameServers {
			var builder strings.Builder
			builder.WriteString(oldNameServerListStr)
			builder.WriteString(value)
			builder.WriteString(":9876;")
			oldNameServerListStr = builder.String()
		}

		if len(oldNameServerListStr) == 0 {
			oldNameServerListStr = share.NameServersStr
		} else {
			oldNameServerListStr = oldNameServerListStr[:len(oldNameServerListStr)-1]
			share.IsNameServersStrUpdated = true
		}

		reqLogger.Info("oldNameServerListStr:" + oldNameServerListStr)

		instance.Status.NameServers = serverNames
		err := r.client.Status().Update(context.TODO(), instance)
		// Update the NameServers status with the host ips
		reqLogger.Info("Updated the NameServers status with the host IP")
		if err != nil {
			reqLogger.Error(err, "Failed to update NameServers status of NameService.")
			return reconcile.Result{Requeue: true}, err
		}

		// use admin tool to update broker config
		if share.IsNameServersStrUpdated && (len(oldNameServerListStr) > cons.MinIpListLength) && (len(share.NameServersStr) > cons.MinIpListLength) {
			mqAdmin := cons.AdminToolDir
			subCmd := cons.UpdateBrokerConfig
			key := cons.ParamNameServiceAddress

			reqLogger.Info("share.GroupNum=broker.Spec.Size=" + strconv.Itoa(share.GroupNum))

			clusterName := share.BrokerClusterName
			reqLogger.Info("Updating config " + key + " of cluster" + clusterName)
			//sh /home/rocketmq/operator/bin/mqadmin updateBrokerConfig -c brokerName -k namesrvAddr -n 1.1.1.1:9876,2.2.2.2:9876 -v 3.3.3.3:9876,4.4.4.4:9876
			cmd := exec.Command("sh", mqAdmin, subCmd, "-c", clusterName, "-k", key, "-n", oldNameServerListStr, "-v", share.NameServersStr)
			output, err := cmd.Output()
			command := mqAdmin + " " + subCmd + " -c " + clusterName + " -k " + key + " -n " + oldNameServerListStr + " -v " + share.NameServersStr
			if err != nil {
				reqLogger.Error(err, "Update Broker config "+key+" failed of cluster "+clusterName+", command: "+command)
				return reconcile.Result{Requeue: true}, err
			}
			reqLogger.Info("Successfully updated Broker config " + key + " of cluster " + clusterName + ", command: " + command + ", with output: " + string(output))
		}

	}
	// Print NameServers addr
	for i, value := range instance.Status.NameServers {
		reqLogger.Info("NameServers IP " + strconv.Itoa(i) + ": " + value)
	}

	runningNameServerNum := getRunningNameServersNum(podList.Items)
	if runningNameServerNum == instance.Spec.Size {
		share.IsNameServersStrInitialized = true
	}

	if requeue {
		return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(cons.RequeueIntervalInSecond) * time.Second}, nil
	}

	return reconcile.Result{}, nil
}

func getNameServers(name, namespace, domain string, size int32) []string {
	if domain == "" {
		domain = "cluster.local"
	}
	var times int32
	var nameServers []string
	for {
		var build strings.Builder
		if times >= size {
			break
		}
		//name-service-0.name-service.namespace.svc.cluster.local
		build.WriteString(name)
		build.WriteString("-")
		build.WriteString(strconv.FormatInt(int64(times), 10))
		build.WriteString(".")
		build.WriteString(name)
		build.WriteString(".")
		build.WriteString(namespace)
		build.WriteString(fmt.Sprintf(".svc.%s", domain))
		nameServers = append(nameServers, build.String())
		times++
	}
	return nameServers
}

func getVolumeClaimTemplates(nameService *rocketmqv1alpha1.NameService) []corev1.PersistentVolumeClaim {
	switch nameService.Spec.StorageMode {
	case cons.StorageModeStorageClass:
		return nameService.Spec.VolumeClaimTemplates
	case cons.StorageModeEmptyDir, cons.StorageModeHostPath:
		fallthrough
	default:
		return nil
	}
}

func getVolumes(nameService *rocketmqv1alpha1.NameService) []corev1.Volume {
	switch nameService.Spec.StorageMode {
	case cons.StorageModeStorageClass:
		return nil
	case cons.StorageModeEmptyDir:
		volumes := []corev1.Volume{{
			Name: nameService.Spec.VolumeClaimTemplates[0].Name,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}}
		return volumes
	case cons.StorageModeHostPath:
		fallthrough
	default:
		volumes := []corev1.Volume{{
			Name: nameService.Spec.VolumeClaimTemplates[0].Name,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: nameService.Spec.HostPath,
				}},
		}}
		return volumes
	}
}

/*func getNameServers(pods []corev1.Pod) []string {
	var nameServers []string
	for _, pod := range pods {
		nameServers = append(nameServers, pod.Status.PodIP)
	}
	return nameServers
}*/

func getRunningNameServersNum(pods []corev1.Pod) int32 {
	var num int32 = 0
	for _, pod := range pods {
		if reflect.DeepEqual(pod.Status.Phase, corev1.PodRunning) {
			num++
		}
	}
	return num
}

func labelsForNameService(name string) map[string]string {
	return map[string]string{"app": "name_service", "name_service_cr": name}
}

func (r *ReconcileNameService) statefulSetForNameService(nameService *rocketmqv1alpha1.NameService) *appsv1.StatefulSet {
	ls := labelsForNameService(nameService.Name)
	dep := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nameService.Name,
			Namespace: nameService.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &nameService.Spec.Size,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					HostNetwork: nameService.Spec.HostNetwork,
					DNSPolicy:   nameService.Spec.DNSPolicy,
					Containers: []corev1.Container{{
						Resources: nameService.Spec.Resources,
						Image:     nameService.Spec.NameServiceImage,
						// Name must be lower case !
						Name:            "name-service",
						ImagePullPolicy: nameService.Spec.ImagePullPolicy,
						Ports: []corev1.ContainerPort{{
							ContainerPort: cons.NameServiceMainContainerPort,
							Name:          cons.NameServiceMainContainerPortName,
						}},
						VolumeMounts: []corev1.VolumeMount{{
							MountPath: cons.LogMountPath,
							Name:      nameService.Spec.VolumeClaimTemplates[0].Name,
							SubPath:   cons.LogSubPathName,
						}},
					}},
					Volumes: getVolumes(nameService),
				},
			},
			VolumeClaimTemplates: getVolumeClaimTemplates(nameService),
		},
	}
	// Set Broker instance as the owner and controller
	controllerutil.SetControllerReference(nameService, dep, r.scheme)

	return dep
}

func (r *ReconcileNameService) ensureExporterStatefulSet(nameService *rocketmqv1alpha1.NameService) *appsv1.StatefulSet {

	var (
		num           = int32(1)
		name          = nameService.Name + "-exporter"
		containerName = "mq-exporter"
		lb            = map[string]string{
			"app": name,
		}
	)

	exporterSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Labels:    lb,
			Name:      name,
			Namespace: nameService.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			Replicas: &num,
			Selector: &metav1.LabelSelector{
				MatchLabels: lb,
			},
			ServiceName: name,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: lb,
				},
				Spec: corev1.PodSpec{
					/*	Affinity: &corev1.Affinity{
						NodeAffinity: &corev1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
								NodeSelectorTerms: []corev1.NodeSelectorTerm{
									{
										MatchExpressions: []corev1.NodeSelectorRequirement{
											{
												Key:      broker.Spec.MqNodeSelectorKey,
												Operator: corev1.NodeSelectorOpIn,
												Values:   []string{broker.Spec.MqNodeSelectorValue},
											},
										},
									},
								},
							},
						},
					},*/
					Containers: []corev1.Container{
						{
							Image:           nameService.Spec.ExporterImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Name:            containerName,
							Args: []string{
								"--rocketmq.config.namesrvAddr=" + strings.Join(getNameServers(
									nameService.Name,
									nameService.Namespace,
									nameService.Spec.K8sClusterDomain,
									nameService.Spec.Size), ";"),
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 5557,
									Protocol:      corev1.ProtocolTCP,
									Name:          containerName,
								},
							},
							Resources: defaultExporterResource(),
						},
					},
				},
			},
		},
	}
	// Set Broker instance as the owner and controller
	controllerutil.SetControllerReference(nameService, exporterSts, r.scheme)
	return exporterSts
}

func (r *ReconcileNameService) newHeadlessService(instance *rocketmqv1alpha1.NameService) interface{} {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name,
			Namespace: instance.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{
					Name:       "cluster",
					Port:       cons.NameServiceMainContainerPort,
					TargetPort: intstr.FromInt(cons.NameServiceMainContainerPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: map[string]string{
				"app": instance.Name,
			},
		},
	}
	// Set Broker instance as the owner and controller
	controllerutil.SetControllerReference(instance, svc, r.scheme)
	return svc
}

/*
func generateNameServerServiceName(nameserverReplicas int32, clusterName, namespace, port, domain string) (brokerId string) {
	parseInt, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		parseInt = 30011
	}
	var (
		sts string
		i int32 = 0
	)
	for ; i < nameserverReplicas; i++ {
		sts += fmt.Sprintf("%v-%v.%v.%v.svc.%s:%v;", clusterName, i, clusterName, namespace, domain, parseInt)
	}
	return sts[0 : len(sts)-1]
}*/

func defaultExporterResource() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"cpu":    resource.MustParse("1500m"),
			"memory": resource.MustParse("3Gi"),
		},
		Requests: corev1.ResourceList{
			"cpu":    resource.MustParse("1000m"),
			"memory": resource.MustParse("2Gi"),
		},
	}
}
