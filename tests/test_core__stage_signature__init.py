import pytest

from typing import Callable

from easypipe.core import StageSignature

def func_noargs():
    ...

def func_only_positional_args(a, b, c, /):
    ...

def func_only_keywords_args(*, a, b, c):
    ...

def func_positional_or_keywords_1(a, b, c):
    ...

def func_positional_or_keywords_2(a, b, /, c):
    ...

def func_positional_or_keywords_3(a, b, *, c):
    ...

def func_starargs(*args):
    ...

def func_starkwargs(**kwargs):
    ...

def func_starargs_and_starkwargs(*args, **kwargs):
    ...

def func_only_positional_args_with_default_1(a, b, c=1, /):
    ...

def func_only_positional_args_with_default_2(a, b=1, c=1, /):
    ...

def func_only_keywords_args_with_default(*, a, b=1, c=1):
    ...

def func_positional_or_keywords_with_default(a, b=1, *, c=1):
    ...

@pytest.mark.parametrize(
    ", ".join([
        "func",
        "exp_num_by_pos",
        "exp_by_keys",
        "exp_by_pos_or_key",
        "exp_has_star_args",
        "exp_has_star_kwargs",
        "exp_num_by_pos_default",
        "exp_by_keys_default",
        "exp_by_pos_or_key_default"
    ]),
    [
        #
        # neither *args nor **kwargs
        #
        (func_noargs, 0, [], [], False, False, 0, [], []),
        (func_only_positional_args, 3, [], [], False, False, 0, [], []),
        (func_only_keywords_args, 0, list("abc"), [], False, False, 0, [], []),
        (func_positional_or_keywords_1, 0, [], list("abc"), False, False, 0, [], []),
        (func_positional_or_keywords_2, 2, [], ["c"], False, False, 0, [], []),
        (func_positional_or_keywords_3, 0, ["c"], list("ab"), False, False, 0, [], []),
        #
        # *args and **kwargs
        #
        (func_starargs, 0, [], [], True, False, 0, [], []),
        (func_starkwargs, 0, [], [], False, True, 0, [], []),
        (func_starargs_and_starkwargs, 0, [], [], True, True, 0, [], []),
        #
        # with defaults
        #
        (func_only_positional_args_with_default_1, 3, [], [], False, False, 1, [], []),
        (func_only_positional_args_with_default_2, 3, [], [], False, False, 2, [], []),
        (func_only_keywords_args_with_default, 0, list("abc"), [], False, False, 0, list("bc"), []),
        (func_positional_or_keywords_with_default, 0, ["c"], list("ab"), False, False, 0, ["c"], ["b"]),
        #
        # lambda
        #
        (lambda a: a, 0, [], ["a"], False, False, 0, [], []),
        (lambda a=1: a, 0, [], ["a"], False, False, 0, [], ["a"]),
        (lambda a, b: a+b, 0, [], list("ab"), False, False, 0, [], []),
    ]
)
def test_stage_inspect_signature(
    func: Callable, 
    exp_num_by_pos: int,
    exp_by_keys: list[str],
    exp_by_pos_or_key: list[str],
    exp_has_star_args: bool,
    exp_has_star_kwargs: bool,
    exp_num_by_pos_default: int,
    exp_by_keys_default: list[str],
    exp_by_pos_or_key_default: list[str]
):
    sig = StageSignature(func)
    assert sig.num_params_by_pos == exp_num_by_pos
    assert sig.by_key == exp_by_keys
    assert sig.has_var_pos == exp_has_star_args
    assert sig.has_var_keys == exp_has_star_kwargs
    assert sig.by_pos_or_key == exp_by_pos_or_key
    assert len(sig.by_pos_with_default) == exp_num_by_pos_default
    assert sig.by_key_with_default == exp_by_keys_default
    assert sig.by_pos_or_key_with_default == exp_by_pos_or_key_default
