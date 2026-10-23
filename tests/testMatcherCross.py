import sys
import unittest

import numpy as np
from fastcluster import linkage
from scipy.spatial.distance import squareform

from mcrit.matchers.MatcherCross import MatcherCross


def _recursive_seriation(Z, N, cur_index):
    # the recursive traversal _seriation used to do, kept as the reference for the order it has to keep
    if cur_index < N:
        return [cur_index]
    return _recursive_seriation(Z, N, int(Z[cur_index - N, 0])) + _recursive_seriation(Z, N, int(Z[cur_index - N, 1]))


class MatcherCrossTest(unittest.TestCase):
    def test_seriation_keeps_the_order_of_the_recursive_traversal(self):
        random = np.random.default_rng(241)
        for num_samples in [2, 3, 5, 17, 60]:
            for method in ["complete", "average", "single", "ward"]:
                with self.subTest(num_samples=num_samples, method=method):
                    points = random.random((num_samples, 4))
                    dist_mat = np.sqrt(((points[:, None, :] - points[None, :, :]) ** 2).sum(axis=-1))
                    Z = linkage(squareform(dist_mat, checks=False), method=method, preserve_input=True)
                    expected = _recursive_seriation(Z, num_samples, 2 * num_samples - 2)
                    self.assertEqual(expected, MatcherCross()._seriation(Z, num_samples, 2 * num_samples - 2))

    def test_cross_compare_of_samples_sharing_no_code_beyond_the_recursion_limit(self):
        # every distance is 100, so the complete linkage is a chain as deep as there are samples
        num_samples = sys.getrecursionlimit() + 100
        reports = [{"info": {"sample": {"sample_id": sample_id}}, "matches": {"samples": []}} for sample_id in range(num_samples)]
        result = MatcherCross()._produce_cross_crompare(reports, cluster_sequence=True)
        self.assertEqual(sorted(str(sample_id) for sample_id in range(num_samples)), sorted(result["clustered_sequence"]))


if __name__ == "__main__":
    unittest.main()
