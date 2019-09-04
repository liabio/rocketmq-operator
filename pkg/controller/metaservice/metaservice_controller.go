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

package metaservice

import (
	"context"
	"os/exec"
	"reflect"
	"strings"

	cachev1alpha1 "github.com/operator-sdk-samples/rocketmq-operator/pkg/apis/cache/v1alpha1"
	rocketmqv1alpha1 "github.com/operator-sdk-samples/rocketmq-operator/pkg/apis/rocketmq/v1alpha1"
	cons "github.com/operator-sdk-samples/rocketmq-operator/pkg/constants"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strconv"
	"time"
)

var log = logf.Log.WithName("controller_metaservice")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new MetaService Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileMetaService{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("metaservice-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource MetaService
	err = c.Watch(&source.Kind{Type: &rocketmqv1alpha1.MetaService{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner MetaService
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &rocketmqv1alpha1.MetaService{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileMetaService implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileMetaService{}

// ReconcileMetaService reconciles a MetaService object
type ReconcileMetaService struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a MetaService object and makes changes based on the state read
// and what is in the MetaService.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileMetaService) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling MetaService")

	// Fetch the MetaService instance
	instance := &rocketmqv1alpha1.MetaService{}
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

	// Check if the deployment already exists, if not create a new one
	found := &appsv1.Deployment{}

	dep := r.deploymentForMetaService(instance)
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: dep.Name, Namespace: dep.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {
		err = r.client.Create(context.TODO(), dep)
		if err != nil {
			reqLogger.Error(err, "Failed to create new Deployment of MetaService", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
		}
		// Deployment created successfully - return and requeue
		// return reconcile.Result{Requeue: true}, nil
	} else if err != nil {
		reqLogger.Error(err, "Failed to get MetaService Deployment.")
	}

	// Ensure the deployment size is the same as the spec
	size := instance.Spec.Size
	if *found.Spec.Replicas != size {
		found.Spec.Replicas = &size
		err = r.client.Update(context.TODO(), found)
		reqLogger.Info("MetaService Updated")
		if err != nil {
			reqLogger.Error(err, "Failed to update Deployment.", "Deployment.Namespace", found.Namespace, "Deployment.Name", found.Name)
			return reconcile.Result{}, err
		}
	}

	return r.updateMetaServiceStatus(instance, request, true)

}

func (r *ReconcileMetaService) updateMetaServiceStatus(instance *rocketmqv1alpha1.MetaService, request reconcile.Request, requeue bool) (reconcile.Result, error){
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Check the MetaServers status")
	// List the pods for this metaService's deployment
	podList := &corev1.PodList{}
	labelSelector := labels.SelectorFromSet(labelsForMetaService(instance.Name))
	listOps := &client.ListOptions{
		Namespace:     instance.Namespace,
		LabelSelector: labelSelector,
	}
	err := r.client.List(context.TODO(), listOps, podList)
	if err != nil {
		reqLogger.Error(err, "Failed to list pods.", "MetaService.Namespace", instance.Namespace, "MetaService.Name", instance.Name)
		return reconcile.Result{}, err
	}
	hostIps := getMetaServers(podList.Items)

	// Update status.MetaServers if needed
	if !reflect.DeepEqual(hostIps, instance.Status.MetaServers) {
		instance.Status.MetaServers = hostIps
		err := r.client.Status().Update(context.TODO(), instance)
		// Update the MetaServers status with the host ips
		reqLogger.Info("Updated the MetaServers status with the host IP")
		if err != nil {
			reqLogger.Error(err, "Failed to update MetaServers status of MetaService.")
			return reconcile.Result{}, err
		}

		metaServerListStr := ""
		for _, value := range instance.Status.MetaServers {
			metaServerListStr = metaServerListStr + value + ":9876;"
		}
		metaServerListStr = strings.TrimLeft(metaServerListStr, ";")
		reqLogger.Info("metaServerListStr:" + metaServerListStr)

		// update namesrvAddr of all brokers
		mqAdmin := cons.MqAdminDir
		subCmd := cons.UpdateBrokerConfig
		key := cons.NamesrvAddr
		broker := &cachev1alpha1.Broker{}
		reqLogger.Info("broker.Spec.Size=" + strconv.Itoa(int(broker.Spec.Size)))
		for i := 0 ;i < int(broker.Spec.Size); i++{
			clusterName := cons.BrokerClusterPrefix + strconv.Itoa(i)
			reqLogger.Info("Updating config " + key + " of cluster" + clusterName)
			cmd := exec.Command("sh", mqAdmin, subCmd, "-c", clusterName, "-k", key, "-n", metaServerListStr, "-v", metaServerListStr)
			output, err := cmd.Output()
			if err != nil {
				reqLogger.Error(err, "Update Broker config " + key + " failed of cluster " + clusterName)
			}
			reqLogger.Info("Successfully updated Broker config " + key + " of cluster " + clusterName + " with output:\n" + string(output))
		}

	}
	// Print MetaServers IP
	for i, value := range instance.Status.MetaServers {
		reqLogger.Info("MetaServers IP " + strconv.Itoa(i) + ": " + value)
	}

	if requeue {
		return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(3)*time.Second}, nil
	} else {
		return reconcile.Result{}, nil
	}
}

func getMetaServers(pods []corev1.Pod) []string {
	var metaServers []string
	for _, pod := range pods {
		metaServers = append(metaServers, pod.Status.HostIP)
	}
	return metaServers
}

func labelsForMetaService(name string) map[string]string {
	return map[string]string{"app": "meta_service", "meta_service_cr": name}
}

func (r *ReconcileMetaService) deploymentForMetaService(m *rocketmqv1alpha1.MetaService) *appsv1.Deployment {
	ls := labelsForMetaService(m.Name)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &m.Spec.Size,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					HostNetwork: true,
					DNSPolicy: "ClusterFirstWithHostNet",
					Containers: []corev1.Container{{
						Image:           m.Spec.MetaServiceImage,
						// Name must be lower case !
						Name:            "meta-service",
						ImagePullPolicy: m.Spec.ImagePullPolicy,
						Ports: []corev1.ContainerPort{{
							ContainerPort: 9876,
							Name:          "9876port",
						}},
						VolumeMounts: []corev1.VolumeMount{{
							MountPath: "/home/rocketmq/logs",
							Name: "namesrvlogs",
						},{
							MountPath: "/home/rocketmq/store",
							Name: "namesrvstore",
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "namesrvlogs",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/data/namesrv/logs",
							},
						},
					},{
						Name: "namesrvstore",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/data/namesrv/store",
							},
						},
					}},
				},
			},
		},
	}
	// Set Broker instance as the owner and controller
	controllerutil.SetControllerReference(m, dep, r.scheme)

	return dep
}