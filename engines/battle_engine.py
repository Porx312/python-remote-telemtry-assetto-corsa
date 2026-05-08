"""
Backward-compatibility facade. Battle logic lives under
``engines.battlesystem`` since the modular refactor; this module re-exports
the public surface so any external imports keep working.
"""

from engines.battlesystem.orchestrator import BattleManager
from engines.battlesystem.pair_manager import PairBattleManager

__all__ = ["BattleManager", "PairBattleManager"]
