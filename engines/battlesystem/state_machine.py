import time

from engines.battlesystem.config import (
    DISAPPEAR_GAP_METERS,
    FINISH_POINT_MIN_GAP_METERS,
    GAP_ABORT_MIN_BOTH_SPEED_KMH,
    MAX_BATTLE_GAP_METERS,
    OVERTAKE_MIN_GAP_METERS,
    OVERTAKE_POINT_COOLDOWN_SEC,
    PAIR_STICKY_TIMEOUT_SEC,
    PRESTART_GAP_ABORT_GRACE_SEC,
    ROLE_ASSIGN_MIN_GAP_SPLINE,
    ROLE_ASSIGN_WAIT_SEC,
    WRONG_POSITION_CHECK_WINDOW_SEC,
    WRONG_POSITION_MARGIN_SPLINE,
)
from engines.battlesystem.models import TougeBattle


def process_pair_logic(manager):
    now = time.time()
    if not manager.is_battle_server:
        return

    active_guids = [g for g, c in manager.cars.items() if (now - c.last_update_time) < 5.0]
    if len(active_guids) < 2:
        if manager.state not in ["IDLE", "FINISHED"]:
            manager._notify_battle_cancelled("not enough players")
            print(f"\n[BATTLE] Not enough players ({len(active_guids)}). Resetting.")
            manager._reset_to_idle(full_reset=True)
        return

    if manager.battle:
        p1 = manager.cars.get(manager.battle.car1_guid)
        p2 = manager.cars.get(manager.battle.car2_guid)
        if not p1 or not p2:
            if manager.state not in ["IDLE", "FINISHED"]:
                manager._notify_battle_cancelled("pair missing")
                print("\n[BATTLE] Active pair missing from car state. Resetting.")
            manager._reset_to_idle(full_reset=True)
            manager.battle = None
            return
        p1_stale = (now - p1.last_update_time) > PAIR_STICKY_TIMEOUT_SEC
        p2_stale = (now - p2.last_update_time) > PAIR_STICKY_TIMEOUT_SEC
        if p1_stale or p2_stale:
            if manager.state == "ACTIVE":
                remaining_guid = None
                if p1_stale and not p2_stale:
                    remaining_guid = manager.battle.car2_guid
                elif p2_stale and not p1_stale:
                    remaining_guid = manager.battle.car1_guid
                if remaining_guid and manager._finalize_default_win(remaining_guid, "opponent_disconnected"):
                    return
            if manager.state not in ["IDLE", "FINISHED"]:
                manager._notify_battle_cancelled("pair stale")
                print("\n[BATTLE] Active pair stale timeout reached. Resetting.")
            manager._reset_to_idle(full_reset=True)
            manager.battle = None
            return
        if manager.battle.car1_guid not in active_guids or manager.battle.car2_guid not in active_guids:
            return
    else:
        pair = manager._pick_candidate_pair(active_guids)
        if not pair:
            return
        manager.battle = TougeBattle(pair[0], pair[1])
        manager._reset_to_idle(full_reset=True)

    if manager.state == "FINISHED":
        if manager.finished_time == 0.0:
            manager.finished_time = now
        elif now - manager.finished_time >= manager.FINISHED_COOLDOWN:
            car1 = manager.cars[manager.battle.car1_guid]
            car2 = manager.cars[manager.battle.car2_guid]
            if (car1.speed < 20.0 and car2.speed < 20.0) or (car1.spline < 0.1 and car2.spline < 0.1):
                print("[BATTLE] Cooldown over & Drivers ready. Ready for a new battle!")
                if manager.battle:
                    manager.battle = TougeBattle(manager.battle.car1_guid, manager.battle.car2_guid)
                manager._reset_to_idle(full_reset=True)
        return

    car1 = manager.cars[manager.battle.car1_guid]
    car2 = manager.cars[manager.battle.car2_guid]
    distance = manager.get_distance(car1.pos, car2.pos)

    if manager.state == "IDLE":
        if distance < 40.0 and (car1.speed >= 25.0 or car2.speed >= 25.0):
            manager.state = "ARMED"
            manager.condition_start_time = now
            print(f"⚡ [BATTLE] ARMED between {car1.guid} and {car2.guid}!")
            if manager.on_chat_message:
                msg = f"[TOUGE] {manager._display_name(car1.guid)} vs {manager._display_name(car2.guid)} | ARMED"
                manager.on_chat_message(car1.guid, msg)
                manager.on_chat_message(car2.guid, msg)

    elif manager.state == "ARMED":
        both_moving = car1.speed >= GAP_ABORT_MIN_BOTH_SPEED_KMH and car2.speed >= GAP_ABORT_MIN_BOTH_SPEED_KMH
        if distance > MAX_BATTLE_GAP_METERS and both_moving and (now - manager.condition_start_time) >= PRESTART_GAP_ABORT_GRACE_SEC:
            manager._abort_run_no_point(f"prestart_gap_{distance:.1f}m")
            return

        if manager.battle_id is None and manager.on_battle_start:
            manager.battle_id = manager.on_battle_start(manager.battle.car1_guid, manager.battle.car2_guid)

        if car1.speed > 40.0 and car2.speed > 40.0:
            manager.state = "LAUNCHING"
            manager.launch_trigger_time = now
            print(f"\n[BATTLE] ROLLING START DETECTED! Gap: {distance:.1f}m. Waiting for both cars to hit 40 km/h...")
            if manager.on_chat_message:
                msg = "[TOUGE] GO — 40+"
                manager.on_chat_message(car1.guid, msg)
                manager.on_chat_message(car2.guid, msg)
        elif now - manager.launch_trigger_time > 3.0 and manager.launch_trigger_time != 0.0:
            print("[BATTLE] Timeout: opponent did not reach 40 km/h within 3s. Cancelling.")
            if manager.on_chat_message:
                msg = "[TOUGE] T-out launch"
                manager.on_chat_message(car1.guid, msg)
                manager.on_chat_message(car2.guid, msg)
            manager._reset_to_idle()

    elif manager.state == "LAUNCHING":
        both_moving = car1.speed >= GAP_ABORT_MIN_BOTH_SPEED_KMH and car2.speed >= GAP_ABORT_MIN_BOTH_SPEED_KMH
        if distance > MAX_BATTLE_GAP_METERS and both_moving and (now - manager.launch_trigger_time) >= PRESTART_GAP_ABORT_GRACE_SEC:
            manager._abort_run_no_point(f"launch_gap_{distance:.1f}m")
            return

        if car1.speed > 40.0 and car2.speed > 40.0:
            if manager.battle.run_count >= 1:
                expected_lead = manager.battle.chase_guid
                expected_chase = manager.battle.lead_guid
                c_lead = manager.cars[expected_lead]
                c_chase = manager.cars[expected_chase]
                jump_gap = (c_chase.spline - c_lead.spline) % 1.0
                if jump_gap < 0.5 and jump_gap > 0.001:
                    nl = manager._display_name(expected_lead)
                    nc = manager._display_name(expected_chase)
                    order_line = f"L {nl} / C {nc}"
                    print(f"🚨 [BATTLE] FALSE START | want {order_line} | chase ahead of lead")
                    manager._abort_run_no_point("false_start")
                    return
            else:
                c1_ahead_gap = (car1.spline - car2.spline) % 1.0
                c2_ahead_gap = (car2.spline - car1.spline) % 1.0
                clear_gap = min(c1_ahead_gap, c2_ahead_gap)
                if clear_gap < ROLE_ASSIGN_MIN_GAP_SPLINE:
                    if (now - manager.launch_trigger_time) <= ROLE_ASSIGN_WAIT_SEC:
                        return
                    manager._abort_run_no_point("leader_not_clear")
                    return

            manager.state = "ACTIVE"
            manager.battle.run_count += 1
            if manager.battle.run_count == 1:
                delta = (car1.spline - car2.spline) % 1.0
                if delta < 0.5:
                    manager.battle.lead_guid = car1.guid
                    manager.battle.chase_guid = car2.guid
                else:
                    manager.battle.lead_guid = car2.guid
                    manager.battle.chase_guid = car1.guid
            else:
                manager.battle.lead_guid, manager.battle.chase_guid = manager.battle.chase_guid, manager.battle.lead_guid

            car1.driven_spline = 0.0
            car2.driven_spline = 0.0
            manager.active_start_time = now
            manager._overtake_chase_scored = False
            manager._last_overtake_point_ts = 0.0

            lead_car = manager.cars[manager.battle.lead_guid]
            chase_car = manager.cars[manager.battle.chase_guid]
            gap = (lead_car.spline - chase_car.spline) % 1.0
            manager.battle.initial_gap_spline = gap if gap < 0.5 else 0.0

            print(f"🔥 [BATTLE] ACTIVE — RUN #{manager.battle.run_count}")
            print(f"   🚩 LEAD:  {manager.battle.lead_guid}")
            print(f"   🦊 CHASE: {manager.battle.chase_guid} | Initial gap: {manager.battle.initial_gap_spline:.4f} spline")
            if manager.on_chat_message:
                manager.on_chat_message(manager.battle.lead_guid, "[TOUGE] LEAD")
                manager.on_chat_message(manager.battle.chase_guid, "[TOUGE] CHASE")

        elif now - manager.launch_trigger_time > 3.0:
            print("[BATTLE] Timeout: opponent did not reach 40 km/h within 3s. Cancelling.")
            if manager.on_chat_message:
                msg = "[TOUGE] T-out launch"
                manager.on_chat_message(car1.guid, msg)
                manager.on_chat_message(car2.guid, msg)
            manager._reset_to_idle()

    elif manager.state == "ACTIVE":
        lead_car = manager.cars[manager.battle.lead_guid]
        chase_car = manager.cars[manager.battle.chase_guid]

        if distance >= DISAPPEAR_GAP_METERS:
            survivor_guid = manager.battle.lead_guid if lead_car.driven_spline >= chase_car.driven_spline else manager.battle.chase_guid
            if manager._finalize_default_win(survivor_guid, "gap_disappeared"):
                return

        if manager.battle.run_count > 1 and (now - manager.active_start_time) <= WRONG_POSITION_CHECK_WINDOW_SEC:
            if chase_car.driven_spline > (lead_car.driven_spline + WRONG_POSITION_MARGIN_SPLINE):
                manager._abort_run_no_point("wrong_position")
                return

        if (now - manager.active_start_time) > 2.0 and (now - manager._last_overtake_point_ts) >= OVERTAKE_POINT_COOLDOWN_SEC:
            if not manager._overtake_chase_scored:
                required_gap = manager.battle.initial_gap_spline + manager.overtake_margin_spline
                if chase_car.driven_spline > (lead_car.driven_spline + required_gap) and distance >= OVERTAKE_MIN_GAP_METERS:
                    print(
                        f"🏎️💨 [BATTLE] OVERTAKE! CHASE ({manager.battle.chase_guid}) "
                        f"cleanly passed LEAD (gap {distance:.1f}m >= {OVERTAKE_MIN_GAP_METERS:.1f}m)."
                    )
                    manager._overtake_chase_scored = True
                    manager._last_overtake_point_ts = now
                    manager._award_point(manager.battle.chase_guid, reason="overtake")
                    return
            else:
                if lead_car.driven_spline > (chase_car.driven_spline + manager.overtake_margin_spline) and distance >= OVERTAKE_MIN_GAP_METERS:
                    print(
                        f"🔁 [BATTLE] RECOVERY! LEAD ({manager.battle.lead_guid}) "
                        f"recovered position (gap {distance:.1f}m >= {OVERTAKE_MIN_GAP_METERS:.1f}m)."
                    )
                    manager._overtake_chase_scored = False
                    manager._last_overtake_point_ts = now
                    manager._award_point(manager.battle.lead_guid, reason="position_recovery")
                    return

        if lead_car.driven_spline >= manager.run_length_spline:
            finish_gap_m = manager.get_distance(lead_car.pos, chase_car.pos)
            is_draw = finish_gap_m < FINISH_POINT_MIN_GAP_METERS
            if is_draw:
                print(f"🏁 [BATTLE] FINISH — DRAW. Gap {finish_gap_m:.1f}m < {FINISH_POINT_MIN_GAP_METERS:.1f}m")
            else:
                print(f"🏁 [BATTLE] FINISH — POINT LEAD. Gap {finish_gap_m:.1f}m >= {FINISH_POINT_MIN_GAP_METERS:.1f}m")
                manager._award_point(manager.battle.lead_guid, reason="finish_outrun")
            manager._finalize_single_session_result(finish_gap_m, is_draw)
            return
