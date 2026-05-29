package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	corev1alpha1 "github.com/Aneesh-Hegde/pb-backup-crd/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type BackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core.pointblank.com,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.pointblank.com,resources=backups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.pointblank.com,resources=backups/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;secrets;configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var backup corev1alpha1.Backup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling Backup definition", "App", backup.Spec.TargetApp)

	// INITIALIZE STATUS
	if len(backup.Status.Conditions) == 0 {
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "Initializing",
			Message: "Starting reconciliation and provisioning",
		})
		if err := r.Status().Update(ctx, &backup); err != nil {
			logger.Error(err, "Failed to update initial status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// DYNAMIC FALLBACKS
	mountPath := backup.Spec.MountPath
	if mountPath == "" {
		mountPath = "/data"
	}
	endpoint := backup.Spec.Endpoint
	if endpoint == "" {
		var garageSvc corev1.Service
		err := r.Get(ctx, types.NamespacedName{Name: "garage", Namespace: "garage"}, &garageSvc)
		if err == nil && len(garageSvc.Spec.Ports) > 0 {
			dynamicPort := garageSvc.Spec.Ports[0].Port
			endpoint = fmt.Sprintf("http://garage.garage.svc.cluster.local:%d", dynamicPort)
		} else {
			endpoint = "http://garage.garage.svc.cluster.local:3906"
		}
	}

	bucketName := backup.Spec.BucketName
	if bucketName == "" {
		bucketName = fmt.Sprintf("%s-backups", backup.Name)
	}

	secretName := backup.Spec.CredentialsSecret

	// ZERO-TOUCH S3 PROVISIONING
	storageProvisioned := meta.IsStatusConditionTrue(backup.Status.Conditions, "StorageProvisioned")

	if secretName == "" {
		secretName = fmt.Sprintf("%s-s3-credentials", backup.Name)

		if !storageProvisioned {
			// Don't re-provision if the K8s secret already exists but status was lost
			var existingSecret corev1.Secret
			secretExists := r.Get(ctx, types.NamespacedName{
				Name: secretName, Namespace: backup.Namespace,
			}, &existingSecret) == nil

			if !secretExists {
				logger.Info("No credentialsSecret provided. Auto-provisioning Garage S3 Bucket and Keys...", "Bucket", bucketName)
				err := r.provisionGarageResources(ctx, backup.Namespace, secretName, bucketName)
				if err != nil {
					meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
						Type:    "StorageProvisioned",
						Status:  metav1.ConditionFalse,
						Reason:  "ProvisioningFailed",
						Message: fmt.Sprintf("Garage API Error: %v", err),
					})
					r.Status().Update(ctx, &backup)
					logger.Error(err, "Failed to auto-provision Garage resources")
					return ctrl.Result{}, err
				}
			}

			// Stamp success on storage
			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:    "StorageProvisioned",
				Status:  metav1.ConditionTrue,
				Reason:  "ProvisioningSucceeded",
				Message: "Garage S3 bucket and secrets successfully created",
			})
			r.Status().Update(ctx, &backup)
			logger.Info("Successfully generated S3 resources and Kubernetes Secret", "SecretName", secretName)
		}
	}

	// DYNAMIC BLUEPRINT FETCHING
	databaseType := backup.Spec.DatabaseType
	if databaseType == "" {
		databaseType = "default"
	}

	blueprintName := fmt.Sprintf("backup-blueprint-%s", databaseType)
	var blueprint corev1.ConfigMap

	err := r.Get(ctx, types.NamespacedName{Name: blueprintName, Namespace: "garage"}, &blueprint)
	if err != nil {
		logger.Error(err, "Failed to locate database backup blueprint", "BlueprintName", blueprintName)
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "BlueprintMissing",
			Message: fmt.Sprintf("Missing engine blueprint ConfigMap '%s' in 'garage' namespace", blueprintName),
		})
		r.Status().Update(ctx, &backup)
		return ctrl.Result{}, err
	}

	appDumpLogic := blueprint.Data["backup.sh"]
	blueprintImage := blueprint.Data["image"]

	image := backup.Spec.Image
	if image == "" && blueprintImage != "" {
		image = blueprintImage
	} else if image == "" {
		image = "amazon/aws-cli:latest"
	}

	// ENVIRONMENT VARIABLES
	envVars := []corev1.EnvVar{
		{
			Name: "AWS_ACCESS_KEY_ID",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  "AWS_ACCESS_KEY_ID",
				},
			},
		},
		{
			Name: "AWS_SECRET_ACCESS_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  "AWS_SECRET_ACCESS_KEY",
				},
			},
		},
		{
			Name:  "AWS_DEFAULT_REGION",
			Value: "us-east-1",
		},
		{
			Name:  "MOUNT_PATH",
			Value: mountPath,
		},
	}

	if len(backup.Spec.DatabaseEnv) > 0 {
		envVars = append(envVars, backup.Spec.DatabaseEnv...)
	}

	s3UploadLogic := fmt.Sprintf(`
echo "Uploading to S3 bucket: %s..."
aws s3 cp /workspace/$FILENAME s3://%s/$FILENAME --endpoint-url %s
`, bucketName, bucketName, endpoint)

	retentionLogic := ""
	if backup.Spec.RetentionDays > 0 {
		retentionLogic = fmt.Sprintf(`
echo "Executing Retention Policy for backups older than %d days..."
CUTOFF=$(date -d "-%d days" +%%s)
aws s3api list-objects --bucket %s --endpoint-url %s | \
  jq -r '.Contents[] | select(.LastModified | fromdateiso8601 < $CUTOFF) | .Key' | \
  while read -r key; do
    if [ -n "$key" ] && [ "$key" != "null" ]; then
      echo "Deleting old backup: $key"
      aws s3 rm s3://%s/"$key" --endpoint-url %s
    fi
  done
`, backup.Spec.RetentionDays, backup.Spec.RetentionDays, bucketName, endpoint, bucketName, endpoint)
	}

	finalScript := fmt.Sprintf(`
set -e
echo "Starting %s backup pipeline for target application: %s..."
TIMESTAMP=$(date +%%Y%%m%%d-%%H%%M%%S)
FILENAME="%s-backup-$TIMESTAMP.tar.gz"

# 1. APPLICATION SPECIFIC BLUEPRINT DUMP
%s

# 2. TARGET STORAGE ENGINE UPLOAD
%s

# 3. CONTEXTUAL RETENTION POLICY ENFORCEMENT
%s

echo "Backup execution lifecycle finalized successfully!"
`, databaseType, backup.Spec.TargetApp, backup.Spec.TargetApp, appDumpLogic, s3UploadLogic, retentionLogic)

	// DEFINE THE CRONJOB
	cronJobName := fmt.Sprintf("%s-cronjob", backup.Name)

	timeZone := "Asia/Kolkata"
	successfulJobsHistoryLimit := int32(3)
	failedJobsHistoryLimit := int32(3)

	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cronJobName,
			Namespace: backup.Namespace,
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   backup.Spec.Schedule,
			TimeZone:                   &timeZone,
			SuccessfulJobsHistoryLimit: &successfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     &failedJobsHistoryLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Affinity: &corev1.Affinity{
								PodAffinity: &corev1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchLabels: map[string]string{
													"app": backup.Spec.TargetApp,
												},
											},
											TopologyKey: "kubernetes.io/hostname",
										},
									},
								},
							},
							Containers: []corev1.Container{{
								Name:    "backup-agent",
								Image:   image,
								Env:     envVars,
								Command: []string{"/bin/sh", "-c"},
								Args:    []string{finalScript},
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("128Mi"),
									},
									Limits: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("500m"),
										corev1.ResourceMemory: resource.MustParse("512Mi"),
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "data-volume",
										MountPath: mountPath,
										ReadOnly:  true,
									},
									{
										Name:      "scratch-volume",
										MountPath: "/workspace",
									},
								},
							}},
							Volumes: []corev1.Volume{
								{
									Name: "data-volume",
									VolumeSource: corev1.VolumeSource{
										PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
											ClaimName: backup.Spec.SourcePVCName,
										},
									},
								},
								{
									Name: "scratch-volume",
									VolumeSource: corev1.VolumeSource{
										EmptyDir: &corev1.EmptyDirVolumeSource{},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(&backup, cronJob, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	// MANAGE THE CRONJOB LIFECYCLE
	var existingCronJob batchv1.CronJob
	err = r.Get(ctx, client.ObjectKey{Name: cronJobName, Namespace: backup.Namespace}, &existingCronJob)

	if err != nil && apierrors.IsNotFound(err) {
		logger.Info("Creating a new CronJob", "CronJob.Namespace", cronJob.Namespace, "CronJob.Name", cronJob.Name)
		err = r.Create(ctx, cronJob)
		if err != nil {
			return ctrl.Result{}, err
		}
	} else if err == nil {
		// Only update if critical logic changed to prevent terminating active jobs
		if existingCronJob.Spec.Schedule != cronJob.Spec.Schedule ||
			existingCronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image != cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image {

			existingCronJob.Spec = cronJob.Spec
			err = r.Update(ctx, &existingCronJob)
			if err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("CronJob spec updated successfully")
		}
	} else {
		return ctrl.Result{}, err
	}

	// FINALIZE STATUS
	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "CronJobSynced",
		Message: "Backup CronJob is provisioned and active",
	})
	if err := r.Status().Update(ctx, &backup); err != nil {
		logger.Error(err, "Failed to update Ready status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.Backup{}).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}

// GARAGE ADMIN API TYPES
type GarageKeyRequest struct {
	Name string `json:"name"`
}

type GarageKeyResponse struct {
	AccessKeyId     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

type GarageBucketRequest struct {
	GlobalAlias string `json:"globalAlias"`
}

type GarageBucketResponse struct {
	ID          string `json:"id"`
	GlobalAlias string `json:"globalAlias"`
}

func (r *BackupReconciler) provisionGarageResources(ctx context.Context, namespace string, secretName string, bucketName string) error {
	logger := log.FromContext(ctx)

	adminEndpoint := os.Getenv("GARAGE_ADMIN_ENDPOINT")
	adminToken := os.Getenv("GARAGE_ADMIN_TOKEN")

	if adminEndpoint == "" || adminToken == "" {
		return fmt.Errorf("GARAGE_ADMIN_ENDPOINT or GARAGE_ADMIN_TOKEN environment variables are missing")
	}

	httpClient := &http.Client{}

	doGarageRequest := func(method, path string, body interface{}) ([]byte, error) {
		var reqBody io.Reader
		if body != nil {
			jsonData, err := json.Marshal(body)
			if err != nil {
				return nil, fmt.Errorf("marshal error: %w", err)
			}
			reqBody = bytes.NewBuffer(jsonData)
		}

		req, err := http.NewRequest(method, adminEndpoint+path, reqBody)
		if err != nil {
			return nil, fmt.Errorf("request build error: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		respData, _ := io.ReadAll(resp.Body)

		// Explicitly catch 409 Conflicts so we don't try to parse empty IDs
		if resp.StatusCode == 409 {
			return respData, fmt.Errorf("409_CONFLICT")
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("garage API error %d: %s", resp.StatusCode, string(respData))
		}
		return respData, nil
	}

	// 1. Create bucket
	bucketData, err := doGarageRequest("POST", "/v1/bucket",
		GarageBucketRequest{GlobalAlias: bucketName})
	if err != nil {
		if err.Error() == "409_CONFLICT" {
			// Bucket already exists — fetch it by alias to get the ID
			logger.Info("Bucket already exists, fetching existing bucket ID", "bucket", bucketName)
			bucketData, err = doGarageRequest("GET",
				fmt.Sprintf("/v1/bucket?alias=%s", bucketName), nil)
			if err != nil {
				return fmt.Errorf("failed to fetch existing bucket: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	var bucketResp GarageBucketResponse
	if err := json.Unmarshal(bucketData, &bucketResp); err != nil {
		// Failsafe: Garage GET /v1/bucket often returns an array. If single unmarshal fails, try array.
		var bucketArray []GarageBucketResponse
		if errArray := json.Unmarshal(bucketData, &bucketArray); errArray == nil && len(bucketArray) > 0 {
			bucketResp = bucketArray[0]
		} else {
			return fmt.Errorf("failed to parse bucket response: %w", err)
		}
	}
	if bucketResp.ID == "" {
		return fmt.Errorf("bucket ID empty in response: %s", string(bucketData))
	}

	// 2. Create key
	keyData, err := doGarageRequest("POST", "/v1/key", GarageKeyRequest{Name: secretName})
	if err != nil {
		if err.Error() == "409_CONFLICT" {
			logger.Info("Key already exists but Kubernetes secret is missing. Auto-healing by regenerating Garage key...", "key", secretName)

			// 2a. Fetch all keys to find the orphaned key ID
			keysData, errList := doGarageRequest("GET", "/v1/key", nil)
			if errList == nil {
				var keysList []struct {
					Name        string `json:"name"`
					AccessKeyId string `json:"accessKeyId"`
				}
				json.Unmarshal(keysData, &keysList)

				// 2b. Wipe the orphaned key
				for _, k := range keysList {
					if k.Name == secretName {
						doGarageRequest("DELETE", fmt.Sprintf("/v1/key?accessKeyId=%s", k.AccessKeyId), nil)
						break
					}
				}
			}

			// 2c. Retry key creation to get fresh credentials
			keyData, err = doGarageRequest("POST", "/v1/key", GarageKeyRequest{Name: secretName})
			if err != nil {
				return fmt.Errorf("failed to regenerate key after cleanup: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create key: %w", err)
		}
	}

	var keyResp GarageKeyResponse
	if err := json.Unmarshal(keyData, &keyResp); err != nil {
		return fmt.Errorf("failed to parse key response: %w", err)
	}

	// 3. Grant permissions
	type GarageAllowRequest struct {
		BucketId    string `json:"bucketId"`
		AccessKeyId string `json:"accessKeyId"`
		Permissions struct {
			Read  bool `json:"read"`
			Write bool `json:"write"`
			Owner bool `json:"owner"`
		} `json:"permissions"`
	}

	allowReq := GarageAllowRequest{
		BucketId:    bucketResp.ID,
		AccessKeyId: keyResp.AccessKeyId,
	}
	allowReq.Permissions.Read = true
	allowReq.Permissions.Write = true
	allowReq.Permissions.Owner = true

	// Garage v1 API strictly expects the Bucket ID in the URL path
	allowEndpoint := fmt.Sprintf("/v1/bucket/%s/allow", bucketResp.ID)
	_, err = doGarageRequest("POST", allowEndpoint, allowReq)
	if err != nil {
		return fmt.Errorf("failed to grant bucket permissions: %w", err)
	}

	// 4. Create Kubernetes Secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     keyResp.AccessKeyId,
			"AWS_SECRET_ACCESS_KEY": keyResp.SecretAccessKey,
		},
	}

	if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create kubernetes secret: %w", err)
	}

	return nil
}
