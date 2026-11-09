from typing import Dict, List

import numpy as np
from fastcluster import linkage
from scipy.spatial.distance import squareform


class MatcherCross:
    def _additional_setup(self):
        self.matcher_type = "MatcherCross"

    def _seriation(self, Z, N, cur_index):
        """
        input:
            - Z is a hierarchical tree (dendrogram)
            - N is the number of points given to the clustering process
            - cur_index is the position in the tree to start the traversal at
        output:
            - order implied by the hierarchical tree Z

        seriation computes the order implied by a hierarchical tree (dendrogram)
        """
        # walk the tree with an explicit stack, left subtree first: the tree can be as deep as it has leaves
        # (samples sharing no code form a chain), which a recursion breaks at Python's recursion limit
        order = []
        stack = [cur_index]
        while stack:
            index = stack.pop()
            if index < N:
                order.append(index)
            else:
                stack.append(int(Z[index - N, 1]))
                stack.append(int(Z[index - N, 0]))
        return order

    def _compute_serial_matrix(self, dist_mat, method="ward"):
        """
        input:
            - dist_mat is a distance matrix
            - method = ["ward","single","average","complete"]
        output:
            - seriated_dist is the input dist_mat,
            but with re-ordered rows and columns
            according to the seriation, i.e. the
            order implied by the hierarchical tree
            - res_order is the order implied by
            the hierarchical tree
            - res_linkage is the hierarchical tree (dendrogram)

        compute_serial_matrix transforms a distance matrix into
        a sorted distance matrix according to the order implied
        by the hierarchical tree (dendrogram)
        """
        N = len(dist_mat)
        flat_dist_mat = squareform(dist_mat)
        res_linkage = linkage(flat_dist_mat, method=method, preserve_input=True)
        res_order = self._seriation(res_linkage, N, N + N - 2)
        seriated_dist = np.zeros((N, N))
        a, b = np.triu_indices(N, k=1)
        seriated_dist[a, b] = dist_mat[[res_order[i] for i in a], [res_order[j] for j in b]]
        seriated_dist[b, a] = seriated_dist[a, b]

        return seriated_dist, res_order, res_linkage

    def _calculate_clustered_sequence(self, matching_percent):
        # convert to dist_mat
        matrix_as_lists = []
        for sample_id, entries in sorted(matching_percent.items()):
            row = []
            for other_sample_id, percent in sorted(entries.items()):
                # we need a symmetric distance, so we use whatever distance is smaller for our sample pair
                distance = min(100 - percent, 100 - matching_percent[other_sample_id][sample_id])
                row.append(distance)
            matrix_as_lists.append(row)
        dist_mat = np.array(matrix_as_lists)
        # calculate clustering
        sorted_matrix, sorted_sequence, linkage = self._compute_serial_matrix(dist_mat, method="complete")
        # map sequence indices to sample_ids
        sample_id_sequence = sorted(matching_percent.keys())
        mapped_sequence = []
        for sorted_id in sorted_sequence:
            mapped_sequence.append(sample_id_sequence[sorted_id])
        return mapped_sequence

    def _produce_cross_crompare(self, matching_reports: List[Dict], method=None, custom_sequence=None, cluster_sequence=False):
        """Generate a n:n cross comparision of all reports provided

        Args:
            matching_reports: matching report (as dict) of type 1vsN for each sample to cross compare
            method: which similarity percentage to use ("unweighted", "score_weighted", "frequency_weighted")
            custom_sequence: use this order of sample_ids
            cluster_sequence: use hierarchical clustering to automatically determine a sequence
        """
        if method is None:
            method = "unweighted"
        sample_id_to_matches = {}
        sample_sequence = []
        # load all reports

        for report in matching_reports:
            sample_id = str(report["info"]["sample"]["sample_id"])
            sample_sequence.append(sample_id)
            sample_id_to_matches[sample_id] = report["matches"]["samples"]

        # in case we have no custom reordering, use input orderering
        if custom_sequence is not None:
            sample_sequence = custom_sequence
            print("Using custom sample sequence:")
            print(sample_sequence)
        sample_id_set = set(sample_sequence)

        matching_matches = {}
        matching_percent = {}
        clustered_sequence = []

        matching_percent = {i: {j: 0 for j in sample_sequence} for i in sample_sequence}
        matching_matches = {i: {j: 0 for j in sample_sequence} for i in sample_sequence}
        for sample_id, matches in sorted(sample_id_to_matches.items()):
            # skip samples which are not part of the cross-compare
            if sample_id not in sample_id_set:
                continue
            for matched_sample in matches:
                other_sample_id = str(matched_sample["sample_id"])
                if other_sample_id in sample_id_set:
                    matching_percent[sample_id][other_sample_id] = matched_sample["matched"]["percent"][method]
                    matching_matches[sample_id][other_sample_id] = matched_sample["matched"]["functions"]["combined"]
            matching_percent[sample_id][sample_id] = 100
        if cluster_sequence:
            clustered_sequence = self._calculate_clustered_sequence(matching_percent)
        return {
            "matching_matches": matching_matches,
            "matching_percent": matching_percent,
            "clustered_sequence": clustered_sequence,
        }

    def create_result(self, matching_reports: List[Dict]):
        MATCHING_METHODS = ["unweighted", "score_weighted", "frequency_weighted", "nonlib_unweighted", "nonlib_score_weighted", "nonlib_frequency_weighted"]

        # potentially define a custom sequence (list of int), sample_id
        # aggregate and produce output
        result = {}
        for matching_method in MATCHING_METHODS:
            result[matching_method] = self._produce_cross_crompare(matching_reports, method=matching_method, cluster_sequence=True)
        return result
