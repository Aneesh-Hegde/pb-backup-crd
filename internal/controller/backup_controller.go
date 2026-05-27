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

	// 1. Fetch the Backup instance
	var backup corev1alpha1.Backup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling Backup definition", "App", backup.Spec.TargetApp)

	// 2. Define the CronJob
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
								Name:  "backup-agent",
								Image: "amazon/aws-cli:latest",
								Command: []string{
									"/bin/sh",
									"-c",
									fmt.Sprintf("echo 'Backing up %s to %s...'", backup.Spec.SourcePVCName, backup.Spec.BucketName),
								},
							}},
						},
					},
				},
			},
		},
	}

	// 3. Set OwnerReference (If Backup is deleted, CronJob is deleted)
	if err := ctrl.SetControllerReference(&backup, cronJob, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Create or Update the CronJob
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
	} else if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.Backup{}).
		Owns(&batchv1.CronJob{}). // Tells the controller to watch CronJobs it owns
		Complete(r)
}
