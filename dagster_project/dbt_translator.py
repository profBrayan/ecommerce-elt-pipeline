"""Liga as sources do dbt (`source('raw', 'x')`) aos assets raw do Dagster
(`raw_x`), para que o grafo raw -> staging -> mart seja uma dependência real de
assets, e não dois grafos desconexos."""
from __future__ import annotations

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator


class RawSourceDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        if dbt_resource_props.get("resource_type") == "source":
            table_name = dbt_resource_props["name"]
            return AssetKey(f"raw_{table_name}")
        return super().get_asset_key(dbt_resource_props)
