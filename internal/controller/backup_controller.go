package controller

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/Aneesh-Hegde/pb-backup-crd/api/v1alpha1"
)

type BackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core.pointblank.com,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.pointblank.com,resources=backups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.pointblank.com,resources=backups/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.Info("🚨 TRIPWIRE: I HAVE A BRAIN!", "Target", req.Name)

	// 1. Fetch the Backup instance
	var backup corev1alpha1.Backup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling Backup definition", "App", backup.Spec.TargetApp)

	// ==========================================
	// 2. DYNAMIC FALLBACKS & DEFAULTS
	// ==========================================
	mountPath := backup.Spec.MountPath
	if mountPath == "" {
		mountPath = "/data"
	}

	image := backup.Spec.Image
	if image == "" {
		image = "amazon/aws-cli:latest"
	}

	endpoint := backup.Spec.Endpoint
	if endpoint == "" {
		endpoint = "http://garage.garage.svc.cluster.local:3906"
	}

	// ==========================================
	// 3. MERGE ENVIRONMENT VARIABLES
	// ==========================================
	envVars := []corev1.EnvVar{
		{
			Name: "AWS_ACCESS_KEY_ID",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: backup.Spec.CredentialsSecret},
					Key:                  "AWS_ACCESS_KEY_ID",
				},
			},
		},
		{
			Name: "AWS_SECRET_ACCESS_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: backup.Spec.CredentialsSecret},
					Key:                  "AWS_SECRET_ACCESS_KEY",
				},
			},
		},
		{
			Name:  "AWS_DEFAULT_REGION",
			Value: "us-east-1",
		},
	}

	// Append custom database credentials if provided
	if len(backup.Spec.DatabaseEnv) > 0 {
		envVars = append(envVars, backup.Spec.DatabaseEnv...)
	}

	// ==========================================
	// 4. CONSTRUCT THE BACKUP SCRIPT
	// ==========================================

	// A. Application Dump Logic
	appDumpLogic := backup.Spec.BackupScript
	if appDumpLogic == "" {
		appDumpLogic = fmt.Sprintf(`
echo "Performing default file-level tar backup..."
tar -czf /tmp/$FILENAME -C %s .
`, mountPath)
	}

	// B. Storage Upload Logic (Operator Controlled)
	s3UploadLogic := fmt.Sprintf(`
echo "Uploading to S3 bucket: %s..."
aws s3 cp /tmp/$FILENAME s3://%s/$FILENAME --endpoint-url %s
`, backup.Spec.BucketName, backup.Spec.BucketName, endpoint)

	// C. Retention/Sharding Logic (Operator Controlled)
	retentionLogic := ""
	if backup.Spec.RetentionDays > 0 {
		retentionLogic = fmt.Sprintf(`
echo "Executing Shard/Prune for backups older than %d days..."
aws s3api list-objects --bucket %s --endpoint-url %s \
  --query "Contents[?LastModified<='\$(date -d '-%d days' -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)'].[Key]" \
  --output text | while read -r key; do
  if [ -n "\$key" ] && [ "\$key" != "None" ]; then
    echo "Deleting old backup: \$key"
    aws s3 rm s3://%s/"\$key" --endpoint-url %s
  fi
done
`, backup.Spec.RetentionDays, backup.Spec.BucketName, endpoint, backup.Spec.RetentionDays, backup.Spec.BucketName, endpoint)
	}

	// D. Combine into final executable script
	finalScript := fmt.Sprintf(`
set -e
echo "Starting backup for %s..."
TIMESTAMP=$(date +%%Y%%m%%d-%%H%%M%%S)
FILENAME="%s-backup-$TIMESTAMP.tar.gz"

# 1. APPLICATION SPECIFIC DUMP
%s

# 2. STORAGE UPLOAD
%s

# 3. RETENTION POLICY
%s

echo "Backup process completed successfully!"
`, backup.Spec.TargetApp, backup.Spec.TargetApp, appDumpLogic, s3UploadLogic, retentionLogic)

	// ==========================================
	// 5. DEFINE THE CRONJOB
	// ==========================================
	cronJobName := fmt.Sprintf("%s-cronjob", backup.Name)
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cronJobName,
			Namespace: backup.Namespace,
		},
		Spec: batchv1.CronJobSpec{
			Schedule: backup.Spec.Schedule,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Containers: []corev1.Container{{
								Name:    "backup-agent",
								Image:   image,
								Env:     envVars,
								Command: []string{"/bin/sh", "-c"},
								Args:    []string{finalScript},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "data-volume",
										MountPath: mountPath,
										ReadOnly:  true,
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
							},
						},
					},
				},
			},
		},
	}

	// ==========================================
	// 6. MANAGE THE CRONJOB LIFECYCLE
	// ==========================================
	if err := ctrl.SetControllerReference(&backup, cronJob, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	var existingCronJob batchv1.CronJob
	err := r.Get(ctx, client.ObjectKey{Name: cronJobName, Namespace: backup.Namespace}, &existingCronJob)

	if err != nil && apierrors.IsNotFound(err) {
		logger.Info("Creating a new CronJob", "CronJob.Namespace", cronJob.Namespace, "CronJob.Name", cronJob.Name)
		err = r.Create(ctx, cronJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("Successfully reconciled CronJob", "Operation", "created")
		return ctrl.Result{}, nil
	} else if err == nil {
		// The CronJob exists! Update it with the newest spec.
		existingCronJob.Spec = cronJob.Spec
		err = r.Update(ctx, &existingCronJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("Successfully updated existing CronJob", "CronJob.Name", cronJob.Name)
		return ctrl.Result{}, nil
	} else {
		return ctrl.Result{}, err
	}
}

func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.Backup{}).
		Owns(&batchv1.CronJob{}). // Tells the controller to watch CronJobs it owns
		Complete(r)
}
