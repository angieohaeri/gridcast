from dataclasses import dataclass

import pandas as pd

TRAIN_START = pd.Timestamp("2024-10-23", tz="UTC")
FIRST_TEST_START = pd.Timestamp("2025-12-01", tz="UTC")
TEST_MONTHS = 3
VAL_MONTHS = 2
N_FOLDS = 4


@dataclass(frozen=True)
class Fold:
    """Half-open windows: train [train_start, val_start), val [val_start, test_start),
    test [test_start, test_end)."""

    train_start: pd.Timestamp
    val_start: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def walk_forward_folds() -> list[Fold]:
    """Expanding-window folds: each test block follows the previous one, and train grows to
    cover everything before its validation window."""
    folds = []
    for i in range(N_FOLDS):
        test_start = FIRST_TEST_START + pd.DateOffset(months=TEST_MONTHS * i)
        folds.append(
            Fold(
                train_start=TRAIN_START,
                val_start=test_start - pd.DateOffset(months=VAL_MONTHS),
                test_start=test_start,
                test_end=test_start + pd.DateOffset(months=TEST_MONTHS),
            )
        )
    return folds
