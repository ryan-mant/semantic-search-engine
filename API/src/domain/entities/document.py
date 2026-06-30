from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class Document:
    content: str
    metadata: Dict[str, Any]
    id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
