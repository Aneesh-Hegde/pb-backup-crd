package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type BackupSpec struct {
	Schedule          string `json:"schedule"`
	TargetApp         string `json:"targetApp"`
	SourcePVCName     string `json:"sourcePVCName"`
	StorageBackend    string `json:"storageBackend"`
	BucketName        string `json:"bucketName"`
	CredentialsSecret string `json:"credentialsSecret"`

	// +kubebuilder:validation:Optional
	Endpoint string `json:"endpoint,omitempty"`

	// +kubebuilder:validation:Optional
	DatabaseEnv []corev1.EnvVar `json:"databaseEnv,omitempty"`

	// +kubebuilder:validation:Optional
	RetentionDays int `json:"retentionDays,omitempty"`
}

type BackupStatus struct {
	LastBackupTime string `json:"lastBackupTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type Backup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupSpec   `json:"spec,omitempty"`
	Status BackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Backup{}, &BackupList{})
}
