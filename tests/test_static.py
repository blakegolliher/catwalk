"""Security and race-safety invariants in the no-build frontend."""

from pathlib import Path


APP_JS = (Path(__file__).parents[1] / "catwalk" / "static" / "app.js").read_text()


def test_frontend_never_injects_html():
    assert ".innerHTML" not in APP_JS
    assert "renderRollupProgress" in APP_JS
    assert "document.createTextNode" in APP_JS


def test_listing_and_rollup_responses_have_generation_guards():
    assert "listingToken" in APP_JS
    assert "token !== listingToken" in APP_JS
    assert "token !== rollupToken" in APP_JS


def test_wide_rollup_does_not_spread_children_as_function_arguments():
    assert "Math.max(1, ...result.children" not in APP_JS
