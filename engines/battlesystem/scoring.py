import time

from engines.battlesystem.config import DEFAULT_WIN_MIN_POINTS, FINISH_POINT_MIN_GAP_METERS
from engines.battlesystem.chat import format_point_broadcast


def score_of(manager, guid):
    if not manager.battle:
        return 0
    if guid == manager.battle.car1_guid:
        return manager.battle.car1_score
    if guid == manager.battle.car2_guid:
        return manager.battle.car2_score
    return 0


def finalize_default_win(manager, winner_guid, reason):
    if manager.state != "ACTIVE" or not manager.battle or not winner_guid:
        return False
    winner_points = score_of(manager, winner_guid)
    if winner_points < DEFAULT_WIN_MIN_POINTS:
        print(f"⚠️ [BATTLE] Default win skipped ({winner_points} < {DEFAULT_WIN_MIN_POINTS}) | reason={reason}")
        manager._reset_to_idle(full_reset=False)
        return True

    manager.battle.winner = winner_guid
    wn = manager._display_name(winner_guid)
    print(
        f"🏆 [BATTLE] DEFAULT WIN {winner_guid} | reason={reason} | "
        f"score={manager.battle.car1_score}-{manager.battle.car2_score}"
    )
    if manager.on_chat_message:
        msg = f"[TOUGE] WIN {wn} DEFAULT ({reason}) | {manager._scoreboard_line()}"
        manager.on_chat_message(manager.battle.car1_guid, msg)
        manager.on_chat_message(manager.battle.car2_guid, msg)
    if manager.on_score_update:
        manager.on_score_update(
            manager.battle_id,
            manager.battle.car1_score,
            manager.battle.car2_score,
            manager.battle.winner,
            manager.battle.points_log,
            manager.battle.car1_guid,
            manager.battle.car2_guid,
        )
    manager.state = "FINISHED"
    return True


def finalize_single_session_result(manager, finish_gap_m, is_draw):
    if manager.state != "ACTIVE":
        return
    if manager.battle.car1_score > manager.battle.car2_score:
        winner = manager.battle.car1_guid
    elif manager.battle.car2_score > manager.battle.car1_score:
        winner = manager.battle.car2_guid
    else:
        winner = None

    manager.battle.winner = winner
    if winner:
        wn = manager._display_name(winner)
        print(f"🏆 [BATTLE] SINGLE SESSION OVER! WINNER: {winner}")
        msg = f"[TOUGE] WIN {wn} | {manager._scoreboard_line()}"
    else:
        print(
            f"🤝 [BATTLE] SINGLE SESSION OVER! DRAW "
            f"(finish gap {finish_gap_m:.1f}m < {FINISH_POINT_MIN_GAP_METERS:.1f}m)"
        )
        msg = f"[TOUGE] DRAW FINAL | {manager._scoreboard_line()}"
    if manager.on_chat_message:
        manager.on_chat_message(manager.battle.car1_guid, msg)
        manager.on_chat_message(manager.battle.car2_guid, msg)
    if manager.on_score_update:
        manager.on_score_update(
            manager.battle_id,
            manager.battle.car1_score,
            manager.battle.car2_score,
            manager.battle.winner,
            manager.battle.points_log,
            manager.battle.car1_guid,
            manager.battle.car2_guid,
        )
    manager.state = "FINISHED"


def award_point(manager, winner_guid, reason="outrun"):
    if winner_guid == manager.battle.car1_guid:
        manager.battle.car1_score += 1
        log_msg = f"Point to {manager.battle.car1_guid} ({reason})"
    elif winner_guid == manager.battle.car2_guid:
        manager.battle.car2_score += 1
        log_msg = f"Point to {manager.battle.car2_guid} ({reason})"
    else:
        log_msg = f"DRAW ({reason})"

    manager.battle.points_log.append({"scorer": winner_guid, "reason": reason, "ts": int(time.time() * 1000)})
    print(f"🏅 {log_msg}. Score: {manager.battle.car1_score} - {manager.battle.car2_score}")

    if manager.on_chat_message:
        msg = format_point_broadcast(manager, winner_guid, reason)
        manager.on_chat_message(manager.battle.car1_guid, msg)
        manager.on_chat_message(manager.battle.car2_guid, msg)
