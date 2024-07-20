import logging

import clearml
import mlflow
import mlflow.entities

LOGGER = logging.getLogger('trackers')
LOGGER.setLevel(level=logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.INFO)


class NullClass:
    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, item):
        return self

    def __setattr__(self, key, value):
        pass

    def __delattr__(self, item):
        pass


class MLflowTask:
    def connect(self, params, name=None):

        def _log_dict(prefix, dict_param):
            for k, v in dict_param.items():
                if isinstance(v, dict):
                    _log_dict(f"{prefix}{k}/", v)
                else:
                    try:
                        mlflow.log_param(f"{prefix}{k}", v)
                    except mlflow.exceptions.MlflowException as exc:
                        if exc.error_code == 'INVALID_PARAMETER_VALUE':
                            LOGGER.debug("Ignoring MLflow 'overwriting a logged parameter' error")
                        else:
                            raise

        if name:
            _log_dict(f"{name}/", params)
        else:
            _log_dict("", params)

    def mark_failed(self, status_message, force):
        mlflow.set_tag("exception", status_message)
        mlflow.end_run('FAILED')

    def close(self):
        mlflow.end_run()


class Trackers:
    CLEARML_ENABLED = True
    MLFLOW_ENABLED = True

    @classmethod
    def init_clearml(cls, clearml_uri):
        # ClearML works via a local config file
        pass

    @classmethod
    def init_mlflow(cls, mlflow_uri):
        mlflow.set_tracking_uri(mlflow_uri)
        cls.mlflow_uri = mlflow_uri

    @classmethod
    def clearml_task_init(cls, project_name, task_name, enabled=None, reuse_trackers=False, **kwargs):
        if enabled is not None:
            cls.CLEARML_ENABLED = enabled
        if not cls.CLEARML_ENABLED:
            return NullClass()
        else:
            return clearml.Task.init(
                project_name=project_name,
                task_name=task_name,
                reuse_last_task_id=reuse_trackers,
                **kwargs)

    @classmethod
    def mlflow_task_init(cls, project_name, task_name, enabled=None, reuse_trackers=False, **kwargs):
        if enabled is not None:
            cls.MLFLOW_ENABLED = enabled

        if not cls.MLFLOW_ENABLED:
            return NullClass()
        else:
            mlflow.autolog()
            experiment = mlflow.get_experiment_by_name(project_name)
            if experiment and experiment.lifecycle_stage == 'deleted':
                raise Exception(
                    f"Colliding deleted experiment with ID {experiment.experiment_id}.  See script comments for remedy")
                # Find the running mlflow server container
                # docker exec -it <container-name-or-id> bash
                # ps axfwww to find the running --backend-store-uri
                # mlflow gc --backend-storage-uri=<from above> --experiment-ids=<experiment ID>

            if experiment and experiment.lifecycle_stage == 'active':
                experiment_id = experiment.experiment_id
            else:
                LOGGER.info("Creating new MLflow experiment %s", project_name)
                experiment_id = mlflow.create_experiment(project_name)

            run_list = []
            if reuse_trackers:
                run_list = mlflow.search_runs([experiment_id],
                                              filter_string=f"run_name='{task_name}'",
                                              order_by=['start_time DESC'],
                                              output_format='list',
                                              run_view_type=mlflow.entities.ViewType.ACTIVE_ONLY)
            if run_list:
                LOGGER.info("Reusuing MLflow run with name %s", run_list[0].info.run_name)
                mlflow.start_run(experiment_id=experiment_id,
                                 log_system_metrics=True,
                                 run_id=run_list[0].info.run_id,
                                 **kwargs)
            else:
                LOGGER.info("Creating new MLflow run with name %s", task_name)
                mlflow.start_run(experiment_id=experiment_id,
                                 log_system_metrics=True,
                                 run_name=task_name,
                                 **kwargs)

            return MLflowTask()

    @classmethod
    def report_scalar(cls, title, series, value, iteration):
        if cls.CLEARML_ENABLED:
            logger = clearml.Logger.current_logger()
            logger.report_scalar(
                title=title, series=series, value=value, iteration=iteration
            )

        if cls.MLFLOW_ENABLED:
            mlflow.log_metric(
                key=f"{title}/{series}", value=value
            )
