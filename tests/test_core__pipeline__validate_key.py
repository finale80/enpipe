import pytest

from typing import Callable, Sequence

from easypipe import make_pipeline

def func_sum(a, b=1):
    return a+b

def func_mul(a, b=1):
    return a*b


@pytest.mark.parametrize(
    ", ".join([
        "funcs",
    ]),[
        ((func_sum, func_mul))
    ]
)
def test__get_item__(
    funcs: Sequence[Callable],
):
    p = make_pipeline(*funcs)
    for idx, f in enumerate(funcs):
        name = f.__name__
        assert p[idx].name == name
        assert p[name].name == name


@pytest.mark.parametrize(
    ", ".join([
        "funcs",
    ]),[
        ((func_sum, func_mul))
    ]
)
def test__convert_key(
    funcs: Sequence[Callable],
):
    p = make_pipeline(*funcs)
    for idx, f in enumerate(funcs):
        name = f.__name__
        assert p._convert_key_to_int(name) == idx
        assert p._convert_key_to_str(idx) == name

    

@pytest.mark.parametrize(
    ", ".join([
        "funcs",
    ]),[
        ((func_sum, func_mul))
    ]
)
def test__get_item__error(
    funcs: Sequence[Callable],
):
    p = make_pipeline(*funcs)
    for key in (len(p), -3, "aaa"):
        with pytest.raises(KeyError) as e:
            p[key]
        assert f"Stage {key} not available" in str(e.value)


@pytest.mark.parametrize(
    ", ".join([
        "funcs",
    ]),[
        ((func_sum, func_mul))
    ]
)
def test__convert_key_error(
    funcs: Sequence[Callable],
):
    p = make_pipeline(*funcs)
    for key in (len(p), -3):
        with pytest.raises(KeyError) as e:
            p._convert_key_to_str(key)
        assert f"Stage {key} not available" in str(e.value)

    for key in ("aaa", "bbb"):
        with pytest.raises(KeyError) as e:
            p._convert_key_to_int(key)
        assert f"Stage {key} not available" in str(e.value)
