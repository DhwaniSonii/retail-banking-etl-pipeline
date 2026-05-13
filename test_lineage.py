"""
Unit Tests — Data Lineage Graph

Tests that the lineage registry is complete and queryable.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from governance.lineage.lineage_graph import LineageGraph, LINEAGE_EDGES, LineageEdge


@pytest.fixture
def graph():
    return LineageGraph()


class TestLineageGraph:

    def test_lineage_registry_not_empty(self):
        assert len(LINEAGE_EDGES) > 0

    def test_all_edges_have_required_fields(self):
        for edge in LINEAGE_EDGES:
            assert edge.source_dataset
            assert edge.source_column
            assert edge.target_dataset
            assert edge.target_column
            assert edge.transformation_rule
            assert edge.pipeline_step

    def test_ancestors_found_for_kpi(self, graph):
        ancestors = graph.ancestors("metrics.kpi_daily_summary", "total_volume_cad")
        assert len(ancestors) > 0
        # Should trace back to TPS source
        source_datasets = {e.source_dataset for e in ancestors}
        assert any("TPS" in ds or "transactions" in ds for ds in source_datasets)

    def test_descendants_found_for_raw_amount(self, graph):
        descendants = graph.descendants("TPS.transactions_raw", "amount")
        assert len(descendants) > 0
        target_datasets = {e.target_dataset for e in descendants}
        assert any("staging" in ds or "marts" in ds or "metrics" in ds for ds in target_datasets)

    def test_export_json_structure(self, graph, tmp_path):
        out = tmp_path / "lineage.json"
        payload = graph.export_json(output_path=out)
        assert "edges" in payload
        assert "total_edges" in payload
        assert payload["total_edges"] == len(LINEAGE_EDGES)
        assert out.exists()

    def test_export_markdown_creates_file(self, graph, tmp_path):
        out = tmp_path / "lineage.md"
        graph.export_markdown(out)
        assert out.exists()
        content = out.read_text()
        assert "| Source Dataset |" in content
        assert len(content.splitlines()) > len(LINEAGE_EDGES)

    def test_no_self_referencing_edges(self):
        for edge in LINEAGE_EDGES:
            assert not (
                edge.source_dataset == edge.target_dataset and
                edge.source_column == edge.target_column
            ), f"Self-referencing edge found: {edge}"
