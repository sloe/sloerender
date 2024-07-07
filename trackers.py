import clearml


class NullClass:
    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, item):
        return self

    def __setattr__(self, key, value):
        pass

    def __delattr__(self, item):
        pass


class Trackers:
    ENABLED = True

    @classmethod
    def init_clearml(cls, clearml_uri):
        # ClearML works via a local config file
        pass

    @classmethod
    def init_mlflow(cls, mlflow_uri):
        cls.mlflow_uri = mlflow_uri

    @classmethod
    def clearml_task_init(cls, project_name, task_name):
        if cls.ENABLED:
            return clearml.Task.init(
                project_name=project_name,
                task_name=task_name,
                reuse_last_task_id=True)
        else:
            return NullClass()

    @classmethod
    def report_scalar(cls, title, series, value, iteration):
        if cls.ENABLED:
            logger = clearml.Logger.current_logger()
            logger.report_scalar(
                title=title, series=series, value=value, iteration=iteration
            )
