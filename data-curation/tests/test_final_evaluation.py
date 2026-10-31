import pytest
from streamsqlbench_curation.final_evaluation import (
    CONTRACT, CSV_NULL_ENCODING, LEGACY_CSV_NULL_ENCODING, FinalContractError,
    evaluate_final, evaluate_final_legacy, read_final_csv, write_final_csv,
)


def contract(**changes):
    return {"contract": CONTRACT, "evaluation_scope": "final_only", "column_binding": "public_names",
            "columns": [{"name": "category", "value_kind": "string"},
                        {"name": "average", "aliases": ["avg_price"], "value_kind": "number"}],
            "answer_policy": "exact_multiset", **changes}


GOLD = {"columns": ["category", "average"], "rows": [["Book", 35], ["Food", "2.88"]]}


def test_explicit_alias_column_and_row_permutation():
    result = evaluate_final({"columns": ["avg_price", "category"], "rows": [["2.880", "Food"], [35.0, "Book"]]}, GOLD, contract())
    assert result["matched"]


@pytest.mark.parametrize("actual", [
    {"columns": ["average", "category"], "rows": [["Book", 35], ["Food", "2.88"]]},
    {"columns": ["EXPR$1", "category"], "rows": [[35, "Book"], [2.88, "Food"]]},
    {"columns": ["category", "category"], "rows": []},
    {"columns": ["category"], "rows": []},
    {"columns": ["category", "average"], "rows": [["Book", "nan"]]},
    {"columns": ["category", "average"], "rows": [["Book", 35], ["Food", "2.88"], ["Book", 35]]},
])
def test_reject_semantic_swap_unknown_alias_bad_schema_and_duplicates(actual):
    assert not evaluate_final(actual, GOLD, contract())["matched"]


def test_string_identifiers_are_not_coerced_to_numbers():
    c = contract(columns=[{"name": "id", "value_kind": "string"}])
    assert not evaluate_final({"columns": ["id"], "rows": [["001"]]}, {"columns": ["id"], "rows": [["1"]]}, c)["matched"]


def test_empty_relation_checks_names():
    c = contract()
    assert evaluate_final({"columns": ["average", "category"], "rows": []}, {"columns": ["category", "average"], "rows": []}, c)["matched"]
    assert not evaluate_final({"columns": ["wrong", "category"], "rows": []}, {"columns": ["category", "average"], "rows": []}, c)["matched"]


def test_ties_require_full_set_unless_explicit_any_one_rule():
    c = contract(columns=[{"name": "publisher"}], answer_policy="exact_multiset")
    gold = {"columns": ["publisher"], "rows": [["A"], ["B"], ["C"]]}
    actual = {"columns": ["publisher"], "rows": [["B"]]}
    assert not evaluate_final(actual, gold, c)["matched"]
    c.update(answer_policy="any_one_argmax", public_tie_rule="Return exactly one of the tied maxima.")
    assert evaluate_final(actual, gold, c)["matched"]
    assert not evaluate_final(gold, gold, c)["matched"]
    assert not evaluate_final({"columns": ["publisher"], "rows": []}, gold, c)["matched"]


def test_gold_error_is_not_candidate_failure():
    with pytest.raises(FinalContractError):
        evaluate_final(GOLD, {"columns": ["unknown"], "rows": []}, contract())


def test_csv_null_is_distinct_from_empty_string(tmp_path):
    p = tmp_path / "x.csv"
    write_final_csv(p, ["a", "b"], [[None, ""]])
    assert read_final_csv(p)["rows"] == [[None, ""]]
    assert read_final_csv(p)["null_encoding"] == CSV_NULL_ENCODING


def test_csv_null_encoding_round_trips_literal_backslashes_and_csv_syntax(tmp_path):
    p = tmp_path / "x.csv"
    rows = [[None, "", r"\N", r"\\N", r"\prefix", "a,b\n\"quoted\""]]
    write_final_csv(p, ["null", "empty", "token", "double", "prefix", "csv"], rows)
    assert read_final_csv(p)["rows"] == rows


def test_legacy_csv_decoder_remains_an_explicit_ambiguous_compatibility_mode(tmp_path):
    p = tmp_path / "legacy.csv"
    p.write_text("a,b\n\\N,\\\\N\n")
    assert read_final_csv(p, null_encoding=LEGACY_CSV_NULL_ENCODING)["rows"] == [[None, r"\\N"]]


@pytest.mark.parametrize("missing", ["column_binding", "answer_policy"])
def test_formal_evaluation_requires_explicit_policies(missing):
    c = contract()
    del c[missing]
    with pytest.raises(FinalContractError, match=missing):
        evaluate_final(GOLD, GOLD, c)
    assert evaluate_final_legacy(GOLD, GOLD, c)["matched"]


def test_unknown_null_encoding_is_rejected(tmp_path):
    p = tmp_path / "x.csv"
    with pytest.raises(FinalContractError, match="unknown CSV null encoding"):
        write_final_csv(p, ["x"], [[None]], null_encoding="unknown")


def test_decimal_38_values_do_not_round_to_a_false_match():
    c = contract(columns=[{"name": "amount", "value_kind": "number"}])
    gold = {"columns": ["amount"], "rows": [["12345678901234567890123456789012345678"]]}
    actual = {"columns": ["amount"], "rows": [["12345678901234567890123456789012345679"]]}
    assert not evaluate_final(actual, gold, c)["matched"]


@pytest.mark.parametrize("actual,expected", [
    ("-0.000", "0"), ("1.00e2", "100"),
    ("123456789012345678901234567890.00", "123456789012345678901234567890"),
])
def test_numeric_equivalence_is_exact_and_context_independent(actual, expected):
    from decimal import localcontext
    c = contract(columns=[{"name": "amount", "value_kind": "number"}])
    with localcontext() as ctx:
        ctx.prec = 3
        assert evaluate_final({"columns": ["amount"], "rows": [[actual]]},
                              {"columns": ["amount"], "rows": [[expected]]}, c)["matched"]


@pytest.mark.parametrize("value", [[["not_a_type"], 1], ["unknown", 1], {"x": 1}])
def test_unsupported_structured_candidate_is_mismatch_not_crash(value):
    c = contract(columns=[{"name": "id", "value_kind": "string"}])
    result = evaluate_final({"columns": ["id"], "rows": [[value]]},
                            {"columns": ["id"], "rows": [["a"]]}, c)
    assert not result["matched"]
    assert result["reason"] == "output_schema_or_type"
