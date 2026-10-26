from lmp_model.splits import N_FOLDS, walk_forward_folds


def test_folds_are_ordered_and_do_not_overlap():
    folds = walk_forward_folds()

    assert len(folds) == N_FOLDS
    for fold in folds:
        assert fold.train_start < fold.val_start < fold.test_start < fold.test_end
    for prev, nxt in zip(folds, folds[1:]):
        assert nxt.test_start == prev.test_end
