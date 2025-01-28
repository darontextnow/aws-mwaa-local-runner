from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator as _KubernetesPodOperator
from de_utils.constants import ENV_IS_LOCAL

LOCAL_KUBE_CONFIG_FILE = "../../kube/kubeconfig.yaml"  # should resolve to the root dir of this project
MWAA_KUBE_CONFIG_FILE = "/usr/local/airflow/dags/kube/kubeconfig.yaml"
KUBE_CONFIG_FILE = LOCAL_KUBE_CONFIG_FILE if ENV_IS_LOCAL else MWAA_KUBE_CONFIG_FILE
CLUSTER_CONTEXT = "aws"


class KubernetesPodOperator(_KubernetesPodOperator):
    """Custom KubernetesPodOperator to simplify the instantiation of this operator by providing our common defaults
    and to add such much needed logging to execute_sync method.

    NOTE: Default is to run in deferred mode. This is preferable for any runs longer than 30s.
        You can override this to run normally (non-deferred) by specifying:
        deferrable=False
    """

    def __init__(
            self,
            *,
            namespace: str | None = "data-science",
            config_file: str = KUBE_CONFIG_FILE,
            in_cluster: bool = False,
            cluster_context: str = CLUSTER_CONTEXT,
            log_events_on_failure: bool = True,
            service_account_name: str | None = "mwaa",
            image_pull_policy: str = "Always",
            startup_timeout_seconds: int = 60 * 5,  # default 5 min
            deferrable: bool = True,
            poll_interval: int = 60,
            **kwargs,
    ) -> None:

        super().__init__(namespace=namespace,
                         config_file=config_file,
                         in_cluster=in_cluster,
                         cluster_context=cluster_context,
                         log_events_on_failure=log_events_on_failure,
                         service_account_name=service_account_name,
                         image_pull_policy=image_pull_policy,
                         startup_timeout_seconds=startup_timeout_seconds,
                         deferrable=False if ENV_IS_LOCAL else deferrable,
                         poll_interval=poll_interval,
                         **kwargs)

    # override to add more logging, particularly the last write out of the pod's status.
    # below is a copy of parent's method with addition of more logging.
    def execute_sync(self, context):
        result = None
        log = context["task"].log
        try:
            self.pod_request_obj = self.build_pod_request_obj(context)
            self.pod = self.get_or_create_pod(  # must set `self.pod` for `on_kill`
                pod_request_obj=self.pod_request_obj,
                context=context,
            )
            # push to xcom now so that if there is an error we still have the values
            ti = context["ti"]
            ti.xcom_push(key="pod_name", value=self.pod.metadata.name)
            ti.xcom_push(key="pod_namespace", value=self.pod.metadata.namespace)

            # Getting remote pod for use in cleanup methods.
            self.remote_pod = self.find_pod(self.pod.metadata.namespace, context=context)
            self.await_pod_start(pod=self.pod)

            if self.get_logs:
                log.info("Retrieving container logs.")
                self.pod_manager.fetch_container_logs(
                    pod=self.pod,
                    container_name=self.base_container_name,
                    follow=True,
                    post_termination_timeout=self.POST_TERMINATION_TIMEOUT,
                )
            else:
                self.pod_manager.await_container_completion(
                    pod=self.pod, container_name=self.base_container_name
                )

            if self.do_xcom_push:
                log.info("Pushing to xcom.")
                self.pod_manager.await_xcom_sidecar_container_start(pod=self.pod)
                result = self.extract_xcom(pod=self.pod)

            log.info("Awaiting pod completion")
            self.remote_pod = self.pod_manager.await_pod_completion(self.pod)
            remote_pod = self.pod_manager.read_pod(self.pod)
            self.log.info("Pod %s has phase %s", self.pod.metadata.name, remote_pod.status.phase)

        finally:
            self.cleanup(
                pod=self.pod or self.pod_request_obj,
                remote_pod=self.remote_pod,
            )
        if self.do_xcom_push:
            return result
