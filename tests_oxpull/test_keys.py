"""Key derivation is pure; no database involved."""

from oxpull.unique import unique_key

ALIAS = "default"
PATH = "tests_oxpull.tasks.unique_echo"


def test_key_is_stable():
    a = unique_key(ALIAS, PATH, "default", [1, "x"], {"a": 1})
    b = unique_key(ALIAS, PATH, "default", [1, "x"], {"a": 1})
    assert a == b


def test_key_is_sha256_hex():
    key = unique_key(ALIAS, PATH, "default", [], {})
    assert len(key) == 64
    assert set(key) <= set("0123456789abcdef")


def test_kwarg_order_is_irrelevant():
    a = unique_key(ALIAS, PATH, "default", [], {"a": 1, "b": 2})
    b = unique_key(ALIAS, PATH, "default", [], {"b": 2, "a": 1})
    assert a == b


def test_nested_dict_order_is_irrelevant():
    a = unique_key(ALIAS, PATH, "default", [], {"outer": {"a": 1, "b": 2}})
    b = unique_key(ALIAS, PATH, "default", [], {"outer": {"b": 2, "a": 1}})
    assert a == b


def test_tuples_equal_lists():
    # normalize_json stores tuples as JSON arrays, so the key does too.
    assert unique_key(ALIAS, PATH, "default", (1, (2, 3)), {}) == unique_key(
        ALIAS, PATH, "default", [1, [2, 3]], {}
    )


def test_positional_order_matters():
    assert unique_key(ALIAS, PATH, "default", [1, 2], {}) != unique_key(
        ALIAS, PATH, "default", [2, 1], {}
    )


def test_queue_is_part_of_the_key():
    assert unique_key(ALIAS, PATH, "default", [1], {}) != unique_key(
        ALIAS, PATH, "emails", [1], {}
    )


def test_task_path_is_part_of_the_key():
    assert unique_key(ALIAS, PATH, "default", [1], {}) != unique_key(
        ALIAS, "tests_oxpull.tasks.other", "default", [1], {}
    )


def test_backend_alias_is_part_of_the_key():
    assert unique_key("default", PATH, "default", [1], {}) != unique_key(
        "other", PATH, "default", [1], {}
    )


def test_int_and_float_differ_like_their_json():
    # 1 and 1.0 store differently in the task row's JSON, so they key
    # differently too. Key equality is JSON equality, nothing smarter.
    assert unique_key(ALIAS, PATH, "default", [1], {}) != unique_key(
        ALIAS, PATH, "default", [1.0], {}
    )


def test_mixed_int_and_str_dict_keys_do_not_raise():
    # JSON coerces int keys to strings when the row is stored; the key
    # derivation applies the same coercion instead of raising over an
    # unsortable mixed-key dict.
    a = unique_key(ALIAS, PATH, "default", [], {"m": {1: "a", "b": 2}})
    b = unique_key(ALIAS, PATH, "default", [], {"m": {"1": "a", "b": 2}})
    assert a == b
