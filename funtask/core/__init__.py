from funtask.core import entities

# https://github.com/pydantic/pydantic/issues/1668#issuecomment-766308566
entities.Func.__pydantic_model__.update_forward_refs()  # type: ignore
