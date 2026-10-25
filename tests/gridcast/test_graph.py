import pandas as pd

from gridcast.graph import correlation_edges


def test_correlation_edges_link_isolated_nodes_within_zone():
    nodes = pd.DataFrame(
        {
            "role": ["fill_in", "fill_in", "fill_in", "attached", "aggregate"],
            "zone": ["COMED", "COMED", "COMED", "AEP", "EXTERNAL"],
        },
        index=pd.Index([1, 2, 3, 4, 5], name="pnode_id"),
    )
    corr = pd.DataFrame(0.1, index=nodes.index, columns=nodes.index)
    corr.loc[1, 2] = corr.loc[2, 1] = 0.9
    corr.loc[1, 4] = 0.99
    corr.loc[5, 4] = 0.8

    edges = correlation_edges(nodes, corr, linked=pd.Index([4]))

    neighbors_of_1 = edges.loc[edges["src"] == 1, "dst"].tolist()
    assert neighbors_of_1 == [2, 3]
    assert (edges["src"] != 4).all()
    assert edges.loc[edges["src"] == 5, "dst"].tolist()[0] == 4
