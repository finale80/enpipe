import pytest

from typing import Callable

from easypipe import make_pipeline

def func_sum(a: float, b: float = 1.0) -> float:
    return a+b

def func_divide(a: float, b: float = 1.0) -> float:
    return a/b

@pytest.mark.parametrize(
    ",".join([
        "funcs",
        "args",
        "kwargs",
        "disable_stages",
        "expected_outputs",
    ]),
    [
        (
            (func_sum, func_divide), 
            (1, ), 
            dict(), 
            (),
            None
        ),
        (
            (func_sum, func_divide), 
            (), 
            {"a": 11, "b": 3},
            (1,),
            (14,)
        ),
        (
            (func_sum, func_divide), 
            (), 
            {"a": 11, "b": 3},
            (0,),
            (11/3,)
        ),
    ]
)
def test_disable(
    funcs: list[Callable],
    args: tuple,
    kwargs: dict,
    disable_stages: list,
    expected_outputs: list | None
):
    p = make_pipeline(*funcs)
    p.disable(*disable_stages)
    
    for idx, stage in enumerate(p):
        is_enabled = (
            len(disable_stages) > 0 
            and idx not in disable_stages
        )
        assert stage.is_enabled == is_enabled

    if expected_outputs is None:
        assert p(*args, **kwargs) is None


@pytest.mark.parametrize(
    ",".join([
        "funcs",
        "args",
        "kwargs",
        "enable_stages",
        "expected_outputs",
    ]),
    [
        (
            (func_sum, func_divide), 
            (1, ), 
            dict(), 
            (),
            None
        ),
        (
            (func_sum, func_divide), 
            (), 
            {"a": 11, "b": 3},
            (1,),
            (11/3,)
        ),
        (
            (func_sum, func_divide), 
            (), 
            {"a": 11, "b": 3},
            (0,),
            (14,)
        ),
    ]
)
def test_enable(
    funcs: list[Callable],
    args: tuple,
    kwargs: dict,
    enable_stages: list,
    expected_outputs: list | None
):
    p = make_pipeline(*funcs)
    p.disable()
    p.enable(*enable_stages)

    for idx, stage in enumerate(p):
        is_enabled = (
            len(enable_stages) > 0 
            and idx in enable_stages
        )
        assert stage.is_enabled == is_enabled

    if expected_outputs is None:
        assert p(*args, **kwargs) is None
