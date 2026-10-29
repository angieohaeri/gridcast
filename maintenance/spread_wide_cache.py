"""Save fold 0's training-window spread (what build_edges correlates) next to the npz cache,
for Colab. Pulling it from the DB over the tailnet takes ~3 hours at the Mac Mini's upload
speed; built here it takes ~1 min and is ~50 MB. Run on the Mac Mini after the spread data
or the fold dates change.
"""

import numpy as np

from lmp_model.dataset import CACHE_PATH
import lmp_model.graph as G
from lmp_model.splits import walk_forward_folds

FOLD = 0

fold = walk_forward_folds()[FOLD]
wide = G.load_spread_wide(fold.train_start, fold.val_start)
np.savez(CACHE_PATH.parent / f"spread_wide_fold{FOLD}.npz", values=wide.to_numpy(), times=wide.index.asi8, pnodes=wide.columns.to_numpy())
