"""Identifiability and past-only CPU estimation, independent of replay reward."""

import simulate_continuous_batch  # noqa: F401
from control_work_search import fit_control


def test_nonnegative_fit_recovers_identifiable_work_cost_and_ignores_future():
    rows = [
        {"features": x, "seconds": 2 + 3 * x[1] + 4 * x[2], "completed": i}
        for i, x in enumerate(((1, 1, 1), (1, 2, 1), (1, 1, 2), (1, 3, 4)))
    ]
    rows.append({"features": (1, 5, 5), "seconds": 10000, "completed": 100})
    prediction = fit_control(rows, (1, 4, 6), 4)
    assert prediction["samples"] == 4
    assert abs(prediction["predicted"] - 38) < 1e-9
    assert all(c >= 0 for c in prediction["coefficients"])
    assert prediction["latest"] == 3
    assert fit_control(rows, (1, 4, 6), 0)["method"] == "mean_until_identifiable"
    assert fit_control([], (1, 4, 6), 0)["predicted"] == 0
