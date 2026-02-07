from __future__ import annotations

from typing import Callable, Any, Self, overload, cast

from collections import OrderedDict, Counter, defaultdict
from dataclasses import dataclass

import functools
import time


def _validate_keys(p: Pipeline, *keys: int|str) -> None:
    for k in keys:
        if isinstance(k, int):
            if (
                k >= len(p)
                or (k < 0 and k < -len(p)) 
            ):
                raise KeyError(f"Stage {k} not available")
        elif k not in p._dict:
            raise KeyError(f"Stage {k} not available")


def validate_key(func: Callable) -> Any:
    @functools.wraps(func)
    def wrapper(p: Pipeline, *keys: int|str) -> Any:
        _validate_keys(p, *keys)
        return func(p, *keys)
    return wrapper


@dataclass
class Stage:
    func: Callable
    name: str = ""
    is_enabled: bool = True

    def __post_init__(self) -> None:
        if self.name == "":
            if isinstance(self.func, functools.partial):
                self.name = f"functools.partial({self.func.func.__name__})"
            else:
                self.name = self.func.__name__
        self._args: tuple = tuple()
        self._kwargs: dict[str, Any] = dict()
        self._out: Any = None

    def __call__(self, *args, **kwargs) -> Any:
        self._args = args
        self._kwargs = kwargs
        if self.is_enabled:
            self._out = self.func(*args, **kwargs)
            return self._out
        return *args, kwargs
    
    def __repr__(self):
        return (
            f"{self.__class__.__name__}(" 
            f"name={self.name!r}, " 
            f"func={self.func.__name__}, "
            f"is_enabled={self.is_enabled}"
            ")"
        )


@dataclass
class StageRun:
    stage: Stage
    inputs: Any
    outputs: Any
    runtime: float


class Pipeline:
    def __init__(
        self, 
        *stages: Stage,
        name: str | None = None,
    ):
        # if a name is duplicated, then add a suffix _<num> to the name
        dupnames = Counter([stage.name for stage in stages])
        cntnames = defaultdict(int)
        self._dict: dict[str, Stage] = OrderedDict()
        for stage in stages:
            if dupnames[stage.name] > 1:
                cntnames[stage.name] += 1
                stage.name += f"_{cntnames[stage.name]}"
            self._dict[stage.name] = stage

        self.name = name if name is not None else ""
        self._run_inputs = []
        self._run_outputs = []
        self._stages_run: list[StageRun] = []

    @property
    def stages(self) -> tuple[Stage, ...]:
        return tuple(self._dict.values())

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._dict.keys())

    def __len__(self) -> int:
        return len(self.stages)

    def __getitem__(self, key: int | str) -> Stage:
        key = cast(str, self._convert_key_to_str(key))
        return self._dict[key]

    def __iter__(self) -> Self:
        self._iter_stages = iter(self.stages)
        return self

    def __next__(self) -> Stage:
        return next(self._iter_stages)

    @validate_key
    def _convert_key_to_int(self, key: int | str) -> int:
        if isinstance(key, int):
            if key >= 0:
                return key
            return len(self) + key
        return self.names.index(key)

    @validate_key
    def _convert_key_to_str(self, key: int | str) -> str:
        if isinstance(key, str):
            return key
        return self.names[key]

    def _run_stage(
        self, 
        stage: Stage, 
        stage_idx: int,
        *args, 
        **kwargs
    ) -> tuple:
        try:
            if stage_idx == 0:
                self._run_inputs.insert(stage_idx, (args, kwargs))
            else:
                self._run_inputs.insert(stage_idx, args)

            t1 = time.perf_counter_ns()
            res = stage(*args, **kwargs)
            t2 = time.perf_counter_ns()

            self._run_outputs.insert(stage_idx, res)
            self._stages_run.insert(
                stage_idx,
                StageRun(
                    stage,
                    inputs=self._run_inputs[stage_idx],
                    outputs=self._run_outputs[stage_idx],
                    runtime=t2-t1,
                )
            )

        except TypeError as e:
            e.add_note(f"--> Error at stage#{stage_idx}({stage.name})")
            raise e
        if res is None:
            res = tuple()
        elif not isinstance(res, tuple):
            res = (res, )
        return res

    def __call__(
        self, 
        *args, 
        stop_at: int | str | None = None,
        start_at: int | str | None = None,
        resume_from: int | str | None = None,
        **kwargs
    ) -> Any:

        # no stage registrered
        if len(self) == 0:
            return None

        first_stage_idx = 0
        if resume_from is not None:
            resume_from = cast(int, self._convert_key_to_int(resume_from))
            if resume_from > 0:
                prev_stage_run = self.get_stages_run(resume_from-1)[0]
                args = prev_stage_run.outputs
                kwargs = dict()
                first_stage_idx = resume_from
        else:
            self._run_inputs = []
            self._run_outputs = []
            self._stages_run = []

            # find first enabled stage
            for idx in range(len(self)):
                if self[idx].is_enabled:
                    break
            # ...and return None if no stage is enabled
            else:
                return None
            first_stage_idx = idx

        if stop_at is not None:
            stop_at = cast(int, self._convert_key_to_int(stop_at))
            if first_stage_idx >= stop_at:
                return None
        else:
            stop_at = len(self)

        # run stages
        _stages = self.stages[first_stage_idx:stop_at]
        next_args = self._run_stage(_stages[0], first_stage_idx, *args, **kwargs)
        for idx, stage in enumerate(_stages[1:], start=first_stage_idx+1):
            next_args = self._run_stage(stage, idx, *next_args)

        if len(next_args) == 1:
            return next_args[0]
        elif len(next_args) == 0:
            return None
        return next_args
    
    @overload
    def get_stages_run(self, *keys: int) -> list[StageRun]:
        ...

    @overload
    def get_stages_run(self, *keys: str) -> list[StageRun]:
        ...

    def get_stages_run(self, *keys):
        """
        Returns StageRun objects excluding disabled stages.
        If not key is provided, returns all StageRun objects.
        """
        if len(keys) == 0:
            return self._stages_run

        keys = {
            self._convert_key_to_str(k)
            for k in keys
        }

        data = [
            run
            for run in self._stages_run
            if run.stage.name in keys
        ]
        return data

    @overload
    @validate_key
    def enable(self, *keys: int) -> None:
        ...

    @overload
    @validate_key
    def enable(self, *keys: str) -> None:
        ...

    @validate_key
    def enable(self, *keys):
        """Enable specific stages"""
        for k in keys:
            self[k].is_enabled = True

    @overload
    @validate_key
    def disable(self, *keys: int) -> None:
        ...

    @overload
    @validate_key
    def disable(self, *keys: str) -> None:
        ...

    @validate_key
    def disable(self, *keys):
        """Disable specific stages (or all stages if no key is specified)"""
        if len(keys) == 0:
            keys = range(0, len(self))
        for k in keys:
            self[k].is_enabled = False

    def __repr__(self) -> str:
        return "".join([
            "Pipeline(",
            ", ".join(map(repr, self.stages)),
            ")"
        ])


def make_pipeline(
    *funcs: Callable
) -> Pipeline:
    stages = [
        Stage(func)
        for func in funcs
    ]
    return Pipeline(*stages)
