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


def test_navigate_tears_down_stale_rollup_work_before_first_await():
    # A failed or superseded navigation must invalidate the previous
    # directory's rollup poll chain instead of letting it repaint the UI.
    nav = APP_JS.split("async function navigate(")[1].split("\nfunction ")[0]
    assert "cancelActiveRollup()" in nav
    assert nav.index("cancelActiveRollup()") < nav.index("await loadPage(")


def test_superseded_loads_are_distinguished_from_failures():
    # A load that lost to a newer one must not wipe the newer render;
    # only genuine failures clear the listing UI.
    assert "return LOAD_SUPERSEDED" in APP_JS
    assert "return LOAD_FAILED" in APP_JS
    assert "clearListingUI()" in APP_JS


def test_rollup_cell_claims_stay_honest():
    # "computing" may only show while a rollup is actually in flight, and the
    # unprovable "no descendant files" claim is gone (complete rollups seed
    # every child dir, so absence means the directory post-dates the rollup).
    assert "rollupInFlight" in APP_JS
    assert "no descendant files" not in APP_JS
