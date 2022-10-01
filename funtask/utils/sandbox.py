import sys
from types import FunctionType, CoroutineType
from typing import Dict, List, Any, Tuple
import importlib


class ModuleManager(dict):
    """
    warning: this can only be used in signal thread environment
    """

    @staticmethod
    def import_with_name(path: str) -> Tuple[Any, List[str]]:
        mdl = importlib.import_module(path)
        if "__all__" in mdl.__dict__:
            items = mdl.__dict__["__all__"]
        else:
            items = [item for item in mdl.__dict__ if not item.startswith("_")]
        return mdl, items

    def __init__(self, module_paths: List[str]):
        super().__init__()
        self.initial_exists_modules = list(sys.modules.keys())
        self.model_item2model = {}
        for module_path in module_paths:
            module, items = self.import_with_name(module_path)
            self.model_item2model.update({
                item: module for item in items
            })

    def drop(self):
        if len(sys.modules) == len(self.initial_exists_modules):
            return
        will_deletes = []
        for module in sys.modules:
            if module not in self.initial_exists_modules:
                will_deletes.append(module)
        for will_delete in will_deletes:
            del sys.modules[will_delete]

    def get(self, key):
        return self.__getitem__(key)

    def __getitem__(self, key: str):
        if key not in self.model_item2model:
            raise NameError(f"name '{key}' is not defined")
        return getattr(self.model_item2model[key], key)

    def __iter__(self):
        return iter(self.model_item2model)

    def __contains__(self, item):
        return item in self.model_item2model


class MergeDict(dict):
    def __init__(self, *dicts: Dict):
        super().__init__()
        self.set_dict = {}
        self.dicts = [*dicts, self.set_dict]

    def __getitem__(self, key: str):
        for d in self.dicts:
            if key in d:
                return d[key]
        raise KeyError(key)

    def __setitem__(self, key, value):
        self.set_dict[key] = value


class UnsafeSandbox:
    def __init__(self, dependencies: List[str] = None, global_: Dict = None):
        super().__init__()
        dependencies = dependencies or []
        self._g = global_ or globals()
        self.module_manager = ModuleManager(dependencies)

    def call_with(self, dependencies: List[str], func, *args, **kwargs) -> Tuple[Any, Dict]:
        locale_module_manager = ModuleManager(dependencies)
        merge_dict = MergeDict(
            locale_module_manager,
            self.module_manager,
            self._g
        )
        f = FunctionType(
            func.__code__,
            merge_dict,
            func.__name__
        )
        res = f(*args, **kwargs)
        locale_module_manager.drop()
        return res, merge_dict.set_dict

    async def async_call_with(self, dependencies: List[str], func, *args, **kwargs) -> Tuple[Any, Dict]:
        locale_module_manager = ModuleManager(dependencies)
        merge_dict = MergeDict(
            locale_module_manager,
            self.module_manager,
            self._g
        )
        f = FunctionType(
            func.__code__,
            merge_dict,
            func.__name__
        )
        res = await f(*args, **kwargs)
        locale_module_manager.drop()
        return res, merge_dict.set_dict

    def drop(self):
        self.module_manager.drop()
