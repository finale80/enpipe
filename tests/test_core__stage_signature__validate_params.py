import pytest

from typing import Callable, Any

from easypipe.core import StageSignature

def func_only_by_pos(a, b, c, /):
    ...

def func_only_by_pos_with_default(a, b, c=1, /):
    ...

def func_by_pos_or_key(a, b, c):
    ...

def func_by_pos_or_key_with_default(a, b, c=1):
    ...

# def func_only_by_key(*, a, b, c):
#     ...
#
# def func_only_by_key_with_default_1(*, a, b, c=1):
#     ...
#
# def func_only_by_key_with_default_2(*, a, b=1, c=2):
#     ...


@pytest.mark.parametrize(
    ", ".join([
        "func",
        "args",
        "kwargs",
    ]),
    [
        (func_only_by_pos, (1,2,3), dict()),
        (func_only_by_pos_with_default, (1,2), dict()),
        (func_by_pos_or_key, (1,2,3), dict()),
        (func_by_pos_or_key_with_default, (1,2,3), dict()),
        (func_by_pos_or_key_with_default, (1,2), dict()),
        # (func_only_by_key_with_default_1, tuple(), {"a":0, "b":1}),
        # (func_only_by_key_with_default_2, tuple(), {"a":0}),
    ]
)
def test_stage_verify_params_valid(
    func: Callable,
    args: tuple[Any],
    kwargs: dict[str, Any],
):
    stage = StageSignature(func)
    stage.validate_params(*args, ) #**kwargs)


@pytest.mark.parametrize(
    ", ".join([
        "func",
        "args",
        "kwargs",
        "err_msg",
    ]),
    [
        (
            func_only_by_pos, (1,), dict(),
            "Too few parameters: expected at least 3 but found 1"
        ),
        (
            func_only_by_pos, (1,2,3,4), dict(),
            "Too many parameters: expected at most 3 but found 4"
        ),
        (
            func_only_by_pos_with_default, (1,), dict(),
            "Too few parameters: expected at least 2 but found 1"
        ),
        (
            func_by_pos_or_key, (1,), dict(),
            "Too few parameters: expected at least 3 but found 1"
        ),
        (
            func_by_pos_or_key, (1,2,3,4), dict(),
            "Too many parameters: expected at most 3 but found 4"
        ),
        (
            func_by_pos_or_key_with_default, (1,), dict(),
            "Too few parameters: expected at least 2 but found 1"
        )
        # (func_only_by_pos_with_default, (1,), dict()),
        # (func_only_by_key, tuple(), dict(zip(list("abcd"), list("1234")))),
        # (func_only_by_key_with_default_1, tuple(), {"a": 1}),
        # (func_only_by_key_with_default_2, tuple(), dict()),
    ]
)
def test_stage_verify_params_exception(
    func: Callable,
    args: tuple[Any],
    kwargs: dict[str, Any],
    err_msg: str,
):
    stage = StageSignature(func)
    with pytest.raises(RuntimeError) as excinfo:
        stage.validate_params(*args, ) #**kwargs)
    assert str(excinfo.value) == err_msg
