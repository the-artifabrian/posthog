import dataclasses
from typing import Optional


@dataclasses.dataclass
class ExportAssetActivityInputs:
    exported_asset_id: int
    source: Optional[str] = None
    limit: Optional[int] = None
    max_height_pixels: Optional[int] = None


@dataclasses.dataclass
class ExportAssetResult:
    exported_asset_id: int
    success: bool
    failure_type: Optional[str] = None
    insight_id: Optional[int] = None
    duration_ms: Optional[float] = None
    export_format: str = ""
    attempts: int = 1


@dataclasses.dataclass
class ExportOutcomeAsset:
    exported_asset_id: int
    success: bool
    failure_type: Optional[str] = None
    duration_ms: Optional[float] = None
    attempts: int = 1
    insight_id: Optional[int] = None
    export_format: str = ""


@dataclasses.dataclass
class EmitExportOutcomeInput:
    team_id: int
    source: str
    assets: list[ExportOutcomeAsset] = dataclasses.field(default_factory=list)
