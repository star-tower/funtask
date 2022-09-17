# FunTask

[![PyPI - Version](https://img.shields.io/pypi/v/funtask.svg)](https://pypi.org/project/funtask)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/funtask.svg)](https://pypi.org/project/funtask)

-----
FunTask is a Python framework for stateful functional task dispatching and execution

- lightweight and easy to use
- scalable and distributed
- stateful
- task is just a `function(status, ...args)`
- observable

```python
from funtask import LocalFunTaskManager, StdLogger, TaskStatus
from typing import List
import asyncio

ft = LocalFunTaskManager(StdLogger())

# increase 10 worker(in different processes) with state: ['new_state']
workers = ft.increase_workers(lambda old_state: ['new_state'], 10)

def function_task(state: List[str], logger: StdLogger, *args):
    state.append("change and visit state in func task")
    logger.log(f"current state is {state}, args: '{' '.join(args)}'")
    return "I'm task result"

# will dispatch function_task to worker0
task = workers[0].dispatch_fun_task(function_task, "i'm", "arguments")

# async wait task result
task_result = asyncio.run(task.get_result())

assert task_result == "I'm task result"
assert task.status is TaskStatus.SUCCESS
print("end.")
```

**Table of Contents**

- [Installation](#installation)
- [License](#license)
- [Features](#features)

## Installation
```console
pip install funtask
```

## License

`funtask` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Features

- [x] local multiprocessing scheduler
- [ ] k8s + celeary task scheduler
- [ ] flet webUI