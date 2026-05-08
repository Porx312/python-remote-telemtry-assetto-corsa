def format_point_broadcast(manager, winner_guid, reason):
    board = manager._scoreboard_line()
    if reason == "draw":
        return f"[TOUGE] DRAW | {board}"
    if reason == "overtake":
        return f"[TOUGE] OVERTAKE | {board}"
    if reason == "position_recovery":
        return f"[TOUGE] RECOVER | {board}"
    if reason == "outrun":
        return f"[TOUGE] OUTRUN | {board}"
    if reason == "dnf_lead_stalled":
        return f"[TOUGE] DNF lead | {board}"
    if reason == "dnf_chase_stalled":
        return f"[TOUGE] DNF chase | {board}"
    if reason == "collision_penalty":
        return f"[TOUGE] HIT rear | {board}"
    if reason == "collision_brake_check":
        return f"[TOUGE] HIT brake | {board}"
    return f"[TOUGE] PT {reason} | {board}"


def notify_battle_cancelled(manager, reason=None):
    if not manager.on_chat_message or not manager.battle:
        return
    msg = f"[TOUGE] BATTLE CANCELLED ({reason})" if reason else "[TOUGE] BATTLE CANCELLED"
    manager.on_chat_message(manager.battle.car1_guid, msg)
    manager.on_chat_message(manager.battle.car2_guid, msg)
