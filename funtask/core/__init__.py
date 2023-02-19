from funtask.core import entities

# https://github.com/pydantic/pydantic/issues/1668#issuecomment-766308566
entities.Func.__pydantic_model__.update_forward_refs()  # type: ignore
entities.Task.__pydantic_model__.update_forward_refs()  # type: ignore
entities.ArgumentStrategy.__pydantic_model__.update_forward_refs()  # type: ignore
entities.WorkerStrategy.__pydantic_model__.update_forward_refs()  # type: ignore
entities.CronTask.__pydantic_model__.update_forward_refs()  # type: ignore
