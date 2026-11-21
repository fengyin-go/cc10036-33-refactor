"""事件解码重构的回归与边界核对脚本（无第三方依赖，直接运行）。

运行：cd backend && .venv/bin/python tests/verify_event_refactor.py

核对范围：
1. 有效输入回归——原始样本、保留率矩阵、统计数据与重构前（legacy 引擎）一致；
2. 两种输入格式同源——标准字段与物理表字段解码结果、批次指纹、矩阵一致；
3. 时间失效边界——单条无法解释的时间让留存整批失败，但不阻断圈选读会员信息；
4. 失败后恢复——持原始批次复跑结果一致，两个批次的分子分母不混进同一份成果。
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from app.analytics_engine import (  # noqa: E402
    EventDecodeError,
    compute_cohort_retention,
    decode_events,
    events_fingerprint,
)
from app.main import _sample_user_events, segment_pipeline  # noqa: E402
from app.services import service_for  # noqa: E402
from app.store import store  # noqa: E402


def _load_legacy_engine():
    """加载重构前冻结的引擎，作为原始口径的对照实现。"""
    spec = importlib.util.spec_from_file_location(
        "legacy_analytics_engine",
        Path(__file__).with_name("legacy_analytics_engine.py"),
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legacy = _load_legacy_engine()

CHECKS = 0


def check(label: str, condition: bool) -> None:
    global CHECKS
    CHECKS += 1
    if not condition:
        raise AssertionError(f"核对失败：{label}")
    print(f"  ✔ {label}")


# ---------------------------------------------------------------------------
# 测试数据
# ---------------------------------------------------------------------------
BASE = 1698768000  # 2023-10-31T00:00:00Z
DAY = 86400


def canonical_batch() -> list[dict]:
    """标准字段格式的有效事件批次（注册 + 回访）。"""
    return [
        {"user_id": "u1", "event_name": "注册", "event_time": BASE + 100},
        {"user_id": "u1", "event_name": "登录", "event_time": BASE + 2 * DAY},
        {"user_id": "u1", "event_name": "登录", "event_time": BASE + 9 * DAY},
        {"user_id": "u2", "event_name": "注册", "event_time": BASE + 200},
        {"user_id": "u2", "event_name": "登录", "event_time": BASE + 8 * DAY},
        {"user_id": "u3", "event_name": "注册", "event_time": BASE + 8 * DAY},
        {"user_id": "u3", "event_name": "登录", "event_time": BASE + 16 * DAY},
        {"user_id": "u4", "event_name": "登录", "event_time": BASE + DAY},  # 无注册，不进群组
        {"user_id": "u5", "event_name": "注册", "event_time": str(BASE + 300)},  # 数值字符串时间
        {"user_id": "", "event_name": "注册", "event_time": BASE},  # 无身份，按原口径跳过
    ]


def physical_batch() -> list[dict]:
    """物理表字段格式（userId/event/ts）的同一批事件。"""
    mapping = {"user_id": "userId", "event_name": "event", "event_time": "ts"}
    return [
        {mapping.get(key, key): value for key, value in record.items()}
        for record in canonical_batch()
    ]


def extended_batch() -> list[dict]:
    """在原始批次上追加一个有效用户的另一批次。"""
    return canonical_batch() + [
        {"user_id": "u6", "event_name": "注册", "event_time": BASE + 400},
        {"user_id": "u6", "event_name": "登录", "event_time": BASE + 3 * DAY},
    ]


def bad_time_record() -> dict:
    """一条无法解释的时间记录。"""
    return {"user_id": "u9", "event_name": "登录", "event_time": "昨天"}


def member_population() -> list[dict]:
    """会员信息人群（标准身份字段 + 会员属性，无事件字段）。"""
    return [
        {"user_id": "m1", "会员等级": "5", "最近登录日期": "2026-09-01"},
        {"user_id": "m2", "会员等级": "2", "最近登录日期": "2026-10-11"},
        {"user_id": "m3", "会员等级": "4", "最近登录日期": "2026-08-20"},
        {"user_id": "m4", "会员等级": "3", "最近登录日期": "2026-09-30"},
    ]


segment_svc = service_for("segment_rule")
cohort_svc = service_for("cohort_analysis")
cache_svc = service_for("result_cache")


def make_rule(name: str, include: str) -> int:
    rule, missing = segment_svc.create_entry({
        "分群名称": name,
        "人群类型": "行为属性组合",
        "筛选条件": include,
        "排除条件": "",
        "交集策略": "all",
    })
    assert not missing, f"建规则缺字段：{missing}"
    return int(rule["id"])


# ---------------------------------------------------------------------------
# 1. 有效输入回归：矩阵、样本、统计与原始口径一致
# ---------------------------------------------------------------------------
def regression_valid_inputs() -> None:
    print("== 1. 有效输入回归（原始样本 / 矩阵 / 统计） ==")
    for label, events in [
        ("手工构造批次", canonical_batch()),
        ("管道默认样本事件", _sample_user_events()),
    ]:
        legacy_matrix = legacy.compute_cohort_retention(events, "注册", "登录", 7, 8)
        new_matrix = compute_cohort_retention(events, "注册", "登录", 7, 8)
        check(f"{label}：群组划分一致", new_matrix.cohort_ids == legacy_matrix.cohort_ids)
        check(f"{label}：周期标签一致", new_matrix.period_labels == legacy_matrix.period_labels)
        check(f"{label}：保留率矩阵一致", new_matrix.retention_matrix == legacy_matrix.retention_matrix)

    # 服务层统计口径与 legacy 矩阵一致
    legacy_matrix = legacy.compute_cohort_retention(canonical_batch(), "注册", "登录", 7, 8)
    result = cohort_svc.run_cohort_analysis(1, canonical_batch())
    expected_avg = round(
        sum(row[0] for row in legacy_matrix.retention_matrix if len(row) > 0)
        / max(len(legacy_matrix.retention_matrix), 1), 2,
    )
    check("服务返回矩阵与原始口径一致", result["retention_matrix"] == legacy_matrix.retention_matrix)
    check("服务返回周1平均保留一致", result["avg_retention_week1"] == expected_avg)
    check("服务返回群组数一致", result["cohort_count"] == len(legacy_matrix.cohort_ids))

    # 圈选样本回归：legacy 条件过滤 vs 重构后服务结果
    include = legacy.parse_segment_conditions("会员等级:gte:3")
    legacy_matched, legacy_count = legacy.segment_population(member_population(), include, [], "all")
    rule_id = make_rule("核对用高等级会员", "会员等级:gte:3")
    seg = segment_svc.compute_segment(rule_id, member_population())
    check("圈选命中数与原始口径一致", seg["matched_count"] == legacy_count == 3)
    check("圈选样本与原始口径一致", seg["sample"] == legacy_matched[:5])
    check("圈选命中率统计一致", seg["match_rate"] == round(legacy_count / 4 * 100, 2))
    check("人群总数统计一致", seg["population_total"] == 4)


# ---------------------------------------------------------------------------
# 2. 两种输入格式同源
# ---------------------------------------------------------------------------
def same_source_two_formats() -> None:
    print("== 2. 两种输入格式同源 ==")
    canonical, physical = canonical_batch(), physical_batch()
    dec_a = decode_events(canonical)
    dec_b = decode_events(physical)
    triples_a = [(d.user_id, d.event_name, d.event_time) for d in dec_a]
    triples_b = [(d.user_id, d.event_name, d.event_time) for d in dec_b]
    check("两种格式解码出的身份/事件/时间一致", triples_a == triples_b)
    check("两种格式批次指纹相同（同源）",
          events_fingerprint(canonical) == events_fingerprint(physical))
    check("不同批次指纹不同",
          events_fingerprint(canonical) != events_fingerprint(extended_batch()))

    matrix_a = compute_cohort_retention(canonical, "注册", "登录", 7, 8)
    matrix_b = compute_cohort_retention(physical, "注册", "登录", 7, 8)
    check("两种格式保留率矩阵一致", matrix_a.retention_matrix == matrix_b.retention_matrix)
    check("两种格式群组划分一致", matrix_a.cohort_ids == matrix_b.cohort_ids)

    # 物理格式人群也能被圈选读到身份（新能力；legacy 读不到，仅作对照记录）
    physical_members = [
        {"userId": "m9", "会员等级": "5"},
        {"userId": "m10", "会员等级": "1"},
    ]
    rule_id = make_rule("核对用物理格式身份", "user_id:eq:m9")
    seg = segment_svc.compute_segment(rule_id, physical_members)
    legacy_inc = legacy.parse_segment_conditions("user_id:eq:m9")
    _, legacy_count = legacy.segment_population(physical_members, legacy_inc, [], "all")
    check("物理格式身份经解码后可圈选", seg["matched_count"] == 1)
    check("原始口径不识别物理格式（对照）", legacy_count == 0)


# ---------------------------------------------------------------------------
# 3. 时间失效边界：留存整批失败，圈选不受阻断
# ---------------------------------------------------------------------------
def invalid_time_boundary() -> None:
    print("== 3. 时间失效边界 ==")
    batch_with_bad = canonical_batch() + [bad_time_record()]

    try:
        decode_events(batch_with_bad)
        raised = None
    except EventDecodeError as exc:
        raised = exc
    check("严格解码对失效时间整批失败", raised is not None)
    check("错误携带批次内位置", raised is not None and raised.index == len(canonical_batch()))

    for bad_value in (float("nan"), float("inf"), "inf", None):
        try:
            decode_events([{"user_id": "u", "event_name": "e", "event_time": bad_value}])
            ok = False
        except EventDecodeError:
            ok = True
        check(f"无法解释的时间 {bad_value!r} 被拒绝", ok)

    # 缺失时间键沿用原口径（默认 0），数值字符串仍可解释
    decoded = decode_events([
        {"user_id": "u", "event_name": "e"},
        {"user_id": "u", "event_name": "e", "event_time": "1698768000"},
    ])
    check("缺失时间键按原口径默认 0", decoded[0].event_time == 0.0)
    check("数值字符串时间仍可解释", decoded[1].event_time == 1698768000.0)

    # 留存计算失败：服务返回失败且状态不推进
    status_before = store.find("cohort_analysis", 3)["status"]
    failed = cohort_svc.run_cohort_analysis(3, batch_with_bad)
    check("留存遇失效时间返回失败", failed["ok"] is False)
    check("失败携带批次内位置", failed["failed_index"] == len(canonical_batch()))
    check("失败后配置状态未推进", store.find("cohort_analysis", 3)["status"] == status_before)

    # 同一批次下圈选已读到会员信息，不被失效时间阻断
    rule_id = make_rule("核对用失效时间批次", "会员等级:gte:3")
    seg = segment_svc.compute_segment(rule_id, member_population() + [bad_time_record()])
    check("圈选读会员信息不被失效时间阻断", seg["ok"] and seg["matched_count"] == 3)


# ---------------------------------------------------------------------------
# 4. 失败后恢复：复跑持原始批次，分子分母不混
# ---------------------------------------------------------------------------
def retry_without_mixing() -> None:
    print("== 4. 失败后恢复与同源边界 ==")
    first = cohort_svc.run_cohort_analysis(1, canonical_batch())
    check("原始批次首次计算成功", first["ok"] is True)

    failed = cohort_svc.run_cohort_analysis(1, canonical_batch() + [bad_time_record()])
    check("含失效时间的批次计算失败", failed["ok"] is False)

    retried = cohort_svc.run_cohort_analysis(1, canonical_batch())
    check("复跑结果矩阵与原始批次一致", retried["retention_matrix"] == first["retention_matrix"])
    check("复跑统计与原始批次一致", retried["avg_retention_week1"] == first["avg_retention_week1"])
    check("复跑群组成员与原始批次一致", retried["cohort_ids"] == first["cohort_ids"])

    other = cohort_svc.run_cohort_analysis(1, extended_batch())
    check("另一批次成果与原始批次不同（未混分子分母）",
          other["ok"] is True and other["retention_matrix"] != first["retention_matrix"])

    # 复跑后圈选报告仍从原始批次核对成员
    rule_id = make_rule("核对用复跑成员", "会员等级:gte:3")
    sample_before = segment_svc.compute_segment(rule_id, member_population())
    cohort_svc.run_cohort_analysis(1, canonical_batch() + [bad_time_record()])  # 再次失败
    sample_after = segment_svc.compute_segment(rule_id, member_population())
    check("复跑后圈选样本仍与原始批次一致", sample_after["sample"] == sample_before["sample"])
    check("复跑后圈选规模仍与原始批次一致",
          sample_after["matched_count"] == sample_before["matched_count"])

    # 管道缓存：按原始批次指纹隔离，失败批次不写入
    hits_before = cache_svc.cache_stats()["total_hits"]
    p1 = segment_pipeline(rule_id=1, events=canonical_batch())
    check("管道有效批次计算成功并写入缓存", p1["cohort"]["ok"] is True and p1["cached"] is True)
    p2 = segment_pipeline(rule_id=1, events=physical_batch())
    hits_after = cache_svc.cache_stats()["total_hits"]
    check("同源物理格式复跑命中同一缓存", hits_after == hits_before + 1)
    check("同源复跑矩阵与原始批次一致", p2["cohort"]["retention_matrix"] == p1["cohort"]["retention_matrix"])
    p3 = segment_pipeline(rule_id=1, events=canonical_batch() + [bad_time_record()])
    check("失败批次不写入缓存", p3["cohort"]["ok"] is False and p3["cached"] is False)
    p4 = segment_pipeline(rule_id=1, events=extended_batch())
    check("另一批次未命中旧缓存（成果不混）",
          p4["cohort"]["ok"] is True and p4["cohort"]["retention_matrix"] != p1["cohort"]["retention_matrix"])
    p5 = segment_pipeline(rule_id=1, events=canonical_batch())
    check("再次复跑仍从原始批次核对成员",
          p5["cohort"]["retention_matrix"] == p1["cohort"]["retention_matrix"])


def main() -> None:
    regression_valid_inputs()
    same_source_two_formats()
    invalid_time_boundary()
    retry_without_mixing()
    print(f"\n全部 {CHECKS} 项核对通过。")


if __name__ == "__main__":
    main()
