import pytest

from easypipe import Stage, Pipeline

from typing import Callable, Any, Sequence

from easypipe.core import make_pipeline

def func_sum(a: float, b: float = 1.0) -> float:
    return a+b

def func_divide(a: float, b: float = 1.0) -> float:
    return a/b

def func_divide_by_zero(a: float, b: float) -> float:
    return a / 0


@pytest.mark.parametrize(
    ", ".join([
        "funcs",
        "args",
        "kwargs",
        "expected"
    ]),
    [
        ((func_sum, func_divide), (1, ), dict(), 2)
    ]
)
def test_call(
    funcs: list[Callable],
    args: tuple[Any],
    kwargs: dict[str, Any],
    expected: Any
):
    p = Pipeline(
        *[Stage(f) for f in funcs]
    )
    assert p(*args, **kwargs) == expected


@pytest.mark.parametrize(
    ", ".join([
        "funcs",
        "stop_at",
        "args",
        "kwargs",
        "expected"
    ]),
    [
        # no stop_at
        (
            (func_sum, func_divide), 
            None,
            (1, ), 
            dict(), 
            2
        ),
        # stop before the first stage (i.e., nothing is run)
        (
            (func_sum, func_sum, func_divide), 
            0,
            (1, ), 
            dict(), 
            None,
        ),
        # exclude last stage
        (
            (func_sum, func_sum, func_divide), 
            -1,
            (1, ), 
            dict(), 
            3,
        )
    ]
)
def test_stop_at(
    funcs: list[Callable],
    stop_at: int | None,
    args: tuple,
    kwargs: dict,
    expected: Any
):
    p = make_pipeline(*funcs)
    assert p(*args, stop_at=stop_at, **kwargs) == expected


@pytest.mark.parametrize(
    ", ".join([
        "funcs",
        "stop_at",
        "resume_from",
        "args",
        "kwargs",
        "expected"
    ]),
    [
        # resume from beginning
        (
            (func_sum, func_sum, func_sum), 
            0,
            0, 
            (1,),
            dict(), 
            4
        ),
        # stop before the first stage (i.e., nothing is run)
        (
            (func_sum, func_sum, func_divide), 
            1,
            1,
            (10, ), 
            dict(), 
            12,
        ),
    ]
)
def test_resume_from(
    funcs: list[Callable],
    stop_at: int,
    resume_from: int,
    args: tuple,
    kwargs: dict,
    expected: Any
):
    p = make_pipeline(*funcs)
    p(*args, stop_at=stop_at, **kwargs)
    assert p(*args, resume_from=resume_from, **kwargs) == expected


@pytest.mark.parametrize(
    ", ".join([
        "funcs",
        "args",
        "kwargs",
        "err_msg"
    ]),
    [
        (
            (func_sum, func_divide_by_zero), 
            (1, ), 
            dict(), 
            "--> Error at stage#1(func_divide_by_zero)"
        )
    ]
)
def test_call_with_error(
    funcs: Sequence[Callable],
    args: tuple[Any],
    kwargs: dict[str, Any],
    err_msg: str
):
    p = Pipeline(
        *[Stage(f) for f in funcs]
    )
    with pytest.raises(Exception, match=r'Error at stage#') as e:
        assert p(*args, **kwargs)
    assert e.exconly().splitlines()[-1] == err_msg
