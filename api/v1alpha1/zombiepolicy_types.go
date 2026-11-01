// Package v1alpha1 contains the ZombiePolicy API.
// +kubebuilder:object:generate=true
// +groupName=zombie.lucabem.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion  = schema.GroupVersion{Group: "zombie.lucabem.io", Version: "v1alpha1"}
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme
)

// ZombiePolicySpec describes which EMR on EKS pods to watch and when to
// consider them zombies.
type ZombiePolicySpec struct {
	// Selector chooses the pods (in the policy's namespace) to watch.
	// Defaults to Spark driver pods created by EMR on EKS.
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`

	// Container is the container whose logs are inspected.
	// +kubebuilder:default=spark-kubernetes-driver
	// +optional
	Container string `json:"container,omitempty"`

	// InactivityThreshold is how long a pod may go without writing a log line
	// before a check counts as a strike.
	// +kubebuilder:default="20m"
	// +optional
	InactivityThreshold metav1.Duration `json:"inactivityThreshold,omitempty"`

	// StrikeInterval is the minimum time between two strikes on the same pod
	// (equivalent to the old Lambda schedule period). Also the re-check period.
	// +kubebuilder:default="5m"
	// +optional
	StrikeInterval metav1.Duration `json:"strikeInterval,omitempty"`

	// MaxStrikes is the number of consecutive strikes after which the EMR job
	// run is cancelled.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxStrikes int32 `json:"maxStrikes,omitempty"`

	// DryRun only emits events and metrics; nothing is cancelled.
	// +optional
	DryRun bool `json:"dryRun,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=zp
// +kubebuilder:printcolumn:name="Threshold",type=string,JSONPath=`.spec.inactivityThreshold`
// +kubebuilder:printcolumn:name="MaxStrikes",type=integer,JSONPath=`.spec.maxStrikes`
// +kubebuilder:printcolumn:name="DryRun",type=boolean,JSONPath=`.spec.dryRun`

// ZombiePolicy is configuration for the watchdog in one namespace: which pods
// to watch and when they count as zombies. It does not describe a desired state
// that anything converges to.
type ZombiePolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ZombiePolicySpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// ZombiePolicyList is a list of ZombiePolicy.
type ZombiePolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZombiePolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ZombiePolicy{}, &ZombiePolicyList{})
}

var _ runtime.Object = &ZombiePolicy{}
