"""多维分析查询引擎：提供跨模块共享的聚合、归因、保留、检测和血缘计算核心。

所有计算方法均为纯函数，不依赖 Store，方便按业务模块组合调用。
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable

# ---------------------------------------------------------------------------
# 通用聚合函数
# ---------------------------------------------------------------------------

AGGREGATORS: dict[str, Callable[[list[float | int]], float]] = {
    "SUM": lambda values: sum(values),
    "AVG": lambda values: sum(values) / len(values) if values else 0.0,
    "COUNT": lambda values: float(len(values)),
    "MAX": lambda values: float(max(values)) if values else 0.0,
    "MIN": lambda values: float(min(values)) if values else 0.0,
    "DISTINCT_COUNT": lambda values: float(len(set(values))),
    "WEIGHTED_AVG": lambda values, weights=None: _weighted_avg(values, weights or []),
}


def _weighted_avg(values: list[float | int], weights: list[float | int]) -> float:
    if not values or not weights or len(values) != len(weights):
        return 0.0
    total_weight = sum(weights)
    if total_weight == 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights)) / total_weight


def compute_metric(values: list[float | int], aggregator: str) -> float:
    """按聚合函数计算指标值，口径统一入口。"""
    func = AGGREGATORS.get(aggregator)
    if func is None:
        raise ValueError(f"不支持的聚合函数：{aggregator}")
    return func(values)


# ---------------------------------------------------------------------------
# 多维立方预计算
# ---------------------------------------------------------------------------

@dataclass
class DimensionCombo:
    """维度组合记录。"""
    dimensions: list[str]
    measure_values: dict[str, float]
    row_count: int = 0


def build_cube_cell(
    raw_data: list[dict[str, Any]],
    dimensions: list[str],
    measures: list[dict[str, str]],  # [{"name": "revenue", "aggregator": "SUM"}, ...]
) -> list[DimensionCombo]:
    """从原始数据构建多维立方单元格，按维度组合分组聚合度量。"""
    group_map: dict[tuple[str, ...], dict[str, list[float | int]]] = {}
    for row in raw_data:
        dim_key = tuple(str(row.get(d, "")) for d in dimensions)
        if dim_key not in group_map:
            group_map[dim_key] = {m["name"]: [] for m in measures}
        for m in measures:
            val = row.get(m["name"])
            if val is not None:
                try:
                    group_map[dim_key][m["name"]].append(float(val))
                except (ValueError, TypeError):
                    pass

    results: list[DimensionCombo] = []
    for dim_key, measure_data in group_map.items():
        measure_values: dict[str, float] = {}
        total_rows = max(len(v) for v in measure_data.values()) if measure_data else 0
        for m in measures:
            values = measure_data.get(m["name"], [])
            measure_values[m["name"]] = compute_metric(values, m["aggregator"])
        results.append(DimensionCombo(
            dimensions=list(dim_key),
            measure_values=measure_values,
            row_count=total_rows,
        ))
    return results


def generate_dimension_combinations(dimensions: list[str]) -> list[list[str]]:
    """生成所有维度组合（用于预计算Cube），含全量组合。"""
    from itertools import combinations
    combos: list[list[str]] = []
    for r in range(1, len(dimensions) + 1):
        for combo in combinations(dimensions, r):
            combos.append(list(combo))
    return combos


# ---------------------------------------------------------------------------
# 分群条件解析
# ---------------------------------------------------------------------------

@dataclass
class FilterCondition:
    field: str
    operator: str  # eq, ne, gt, lt, gte, lte, in, not_in, contains, starts_with
    value: Any


def parse_segment_conditions(condition_expr: str) -> list[FilterCondition]:
    """解析分群筛选条件表达式。格式：field:op:value;field:op:value"""
    conditions: list[FilterCondition] = []
    if not condition_expr:
        return conditions
    for part in condition_expr.split(";"):
        part = part.strip()
        if not part:
            continue
        components = part.split(":", 2)
        if len(components) >= 3:
            conditions.append(FilterCondition(
                field=components[0].strip(),
                operator=components[1].strip(),
                value=components[2].strip(),
            ))
    return conditions


def apply_filter(row: dict[str, Any], condition: FilterCondition) -> bool:
    """对单行数据应用筛选条件。"""
    cell_val = row.get(condition.field)
    op = condition.operator
    val = condition.value

    try:
        if op == "eq":
            return str(cell_val) == str(val)
        elif op == "ne":
            return str(cell_val) != str(val)
        elif op in ("gt", "gte", "lt", "lte"):
            cell_num = float(cell_val) if cell_val is not None else 0
            val_num = float(val)
            if op == "gt":
                return cell_num > val_num
            elif op == "gte":
                return cell_num >= val_num
            elif op == "lt":
                return cell_num < val_num
            elif op == "lte":
                return cell_num <= val_num
        elif op == "in":
            allowed = set(v.strip() for v in str(val).split(","))
            return str(cell_val) in allowed
        elif op == "not_in":
            denied = set(v.strip() for v in str(val).split(","))
            return str(cell_val) not in denied
        elif op == "contains":
            return str(val).casefold() in str(cell_val).casefold()
        elif op == "starts_with":
            return str(cell_val or "").startswith(str(val))
        elif op == "between":
            bounds = str(val).split(",")
            if len(bounds) == 2:
                cell_num = float(cell_val) if cell_val is not None else 0
                return float(bounds[0]) <= cell_num <= float(bounds[1])
        return False
    except (ValueError, TypeError):
        return False


def segment_population(
    population: list[dict[str, Any]],
    include_conditions: list[FilterCondition],
    exclude_conditions: list[FilterCondition],
    intersect_strategy: str = "all",  # all / any
) -> tuple[list[dict[str, Any]], int]:
    """对人群执行分群圈选，返回命中的行和命中数。"""
    results: list[dict[str, Any]] = []
    for row in population:
        if include_conditions:
            if intersect_strategy == "all":
                if not all(apply_filter(row, c) for c in include_conditions):
                    continue
            else:
                if not any(apply_filter(row, c) for c in include_conditions):
                    continue
        if exclude_conditions and any(apply_filter(row, c) for c in exclude_conditions):
            continue
        results.append(row)
    return results, len(results)


# ---------------------------------------------------------------------------
# 同期群保留率计算
# ---------------------------------------------------------------------------

@dataclass
class CohortMatrix:
    """同期群矩阵：行=同期群（按初始事件时间划分），列=周期偏移。"""
    cohort_ids: list[str]
    period_labels: list[str]
    retention_matrix: list[list[float]]  # [cohort_index][period_index] = 保留率


def compute_cohort_retention(
    events: list[dict[str, Any]],
    init_event_name: str,
    return_event_name: str,
    period_days: int = 7,
    max_periods: int = 8,
) -> CohortMatrix:
    """计算同期群保留率矩阵。

    events: 每行 {user_id, event_name, event_time(epoch_seconds)}
    返回保留率矩阵（百分比 0-100）。
    """
    # 按user分组事件
    user_events: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for e in events:
        uid = str(e.get("user_id", ""))
        ename = str(e.get("event_name", ""))
        etime = float(e.get("event_time", 0))
        if uid:
            user_events[uid].append((ename, etime))

    # 为每个用户找到首次初始事件时间和回访事件时间
    user_cohorts: dict[str, float] = {}  # user -> first init time
    user_return_times: dict[str, list[float]] = defaultdict(list)
    for uid, evts in user_events.items():
        first_init: float | None = None
        returns: list[float] = []
        for ename, etime in sorted(evts, key=lambda x: x[1]):
            if first_init is None and ename == init_event_name:
                first_init = etime
            elif first_init is not None and ename == return_event_name and etime > first_init:
                returns.append(etime)
        if first_init is not None:
            user_cohorts[uid] = first_init
            user_return_times[uid] = returns

    # 按周分组同期群
    period_seconds = period_days * 86400
    cohort_buckets: dict[int, set[str]] = defaultdict(set)  # week_number -> set of user_ids
    for uid, init_time in user_cohorts.items():
        week = int(init_time // period_seconds)
        cohort_buckets[week].add(uid)

    sorted_weeks = sorted(cohort_buckets)
    cohort_ids = [_week_label(w, period_days) for w in sorted_weeks]
    matrix: list[list[float]] = []

    for wi, week in enumerate(sorted_weeks):
        users_in_cohort = cohort_buckets[week]
        total = len(users_in_cohort)
        if total == 0:
            matrix.append([0.0] * max_periods)
            continue
        cohort_start = week * period_seconds
        period_rates: list[float] = []
        for p in range(max_periods):
            period_end = cohort_start + (p + 1) * period_seconds
            retained = 0
            for uid in users_in_cohort:
                return_times = user_return_times.get(uid, [])
                if any(cohort_start < rt <= period_end for rt in return_times):
                    retained += 1
            rate = round(retained / total * 100, 2)
            period_rates.append(rate)
        matrix.append(period_rates)

    period_labels = [f"周{i + 1}" for i in range(max_periods)]
    return CohortMatrix(
        cohort_ids=cohort_ids,
        period_labels=period_labels,
        retention_matrix=matrix,
    )


def _week_label(epoch_seconds: float, period_days: int) -> str:
    import datetime
    dt = datetime.datetime.fromtimestamp(epoch_seconds, tz=datetime.timezone.utc)
    return dt.strftime("W%W-%Y")


# ---------------------------------------------------------------------------
# 归因权重分配
# ---------------------------------------------------------------------------

@dataclass
class AttributionResult:
    """归因结果：每个触点的权重分配。"""
    touchpoints: list[str]
    weights: list[float]
    model: str  # first_touch / last_touch / linear / time_decay / position_based


def compute_attribution(
    touchpoints: list[dict[str, Any]],
    model: str = "linear",
    decay_half_life_days: float = 7.0,
) -> AttributionResult:
    """计算归因权重分配。

    touchpoints: [{"name": str, "time": epoch_seconds}, ...]
    模型支持：first_touch, last_touch, linear, time_decay, position_based
    """
    if not touchpoints:
        return AttributionResult(touchpoints=[], weights=[], model=model)

    tp_names = [tp["name"] for tp in touchpoints]
    tp_times = [float(tp.get("time", 0)) for tp in touchpoints]
    n = len(touchpoints)

    if model == "first_touch":
        weights = [1.0 if i == 0 else 0.0 for i in range(n)]
    elif model == "last_touch":
        weights = [1.0 if i == n - 1 else 0.0 for i in range(n)]
    elif model == "time_decay":
        max_time = max(tp_times) if tp_times else 0
        half_life = decay_half_life_days * 86400
        raw: list[float] = []
        for t in tp_times:
            delta = max_time - t
            raw.append(2.0 ** (-delta / half_life) if half_life > 0 else 1.0)
        raw_sum = sum(raw)
        weights = [r / raw_sum if raw_sum > 0 else 1.0 / n for r in raw]
    elif model == "position_based":
        if n == 1:
            weights = [1.0]
        else:
            first_weight = 0.4
            last_weight = 0.4
            middle_share = 0.2
            middle_count = n - 2
            weights = []
            for i in range(n):
                if i == 0:
                    weights.append(first_weight)
                elif i == n - 1:
                    weights.append(last_weight)
                else:
                    weights.append(middle_share / middle_count if middle_count > 0 else 0)
    else:  # linear
        weights = [1.0 / n] * n

    # 归一化
    total = sum(weights)
    if total > 0:
        weights = [w / total for w in weights]

    return AttributionResult(touchpoints=tp_names, weights=weights, model=model)


# ---------------------------------------------------------------------------
# 血缘图结构计算
# ---------------------------------------------------------------------------

@dataclass
class LineageNode:
    node_id: str
    node_name: str
    node_type: str  # table / metric / column / report
    upstream_ids: list[str]
    downstream_ids: list[str]


@dataclass
class LineageGraph:
    nodes: dict[str, LineageNode] = field(default_factory=dict)

    def add_node(self, node: LineageNode) -> None:
        self.nodes[node.node_id] = node

    def upstream_chain(self, node_id: str, max_depth: int = 10) -> list[list[str]]:
        """获取上游链路（从当前节点到根源的所有路径）。"""
        if node_id not in self.nodes:
            return []
        chains: list[list[str]] = []
        stack: list[tuple[str, list[str]]] = [(node_id, [node_id])]
        while stack:
            current, path = stack.pop()
            if len(path) > max_depth:
                chains.append(list(path))
                continue
            node = self.nodes.get(current)
            if node and node.upstream_ids:
                upstream_found = False
                for up_id in node.upstream_ids:
                    if up_id not in path:
                        stack.append((up_id, path + [up_id]))
                        upstream_found = True
                if not upstream_found:
                    chains.append(list(path))
            else:
                chains.append(list(path))
        return chains

    def downstream_chain(self, node_id: str, max_depth: int = 10) -> list[list[str]]:
        """获取下游影响链。"""
        if node_id not in self.nodes:
            return []
        chains: list[list[str]] = []
        stack: list[tuple[str, list[str]]] = [(node_id, [node_id])]
        while stack:
            current, path = stack.pop()
            if len(path) > max_depth:
                chains.append(list(path))
                continue
            node = self.nodes.get(current)
            if node and node.downstream_ids:
                ds_found = False
                for ds_id in node.downstream_ids:
                    if ds_id not in path:
                        stack.append((ds_id, path + [ds_id]))
                        ds_found = True
                if not ds_found:
                    chains.append(list(path))
            else:
                chains.append(list(path))
        return chains

    def impact_analysis(self, node_id: str) -> list[str]:
        """计算某个节点变更影响的所有下游节点。"""
        visited: set[str] = set()
        queue: deque[str] = deque([node_id])
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            node = self.nodes.get(current)
            if node:
                for ds in node.downstream_ids:
                    if ds not in visited:
                        queue.append(ds)
        visited.discard(node_id)
        return sorted(visited)

    def to_adjacency(self) -> dict[str, Any]:
        """导出邻接表格式。"""
        adj: dict[str, list[str]] = {}
        for nid, node in self.nodes.items():
            adj[nid] = list(node.upstream_ids)
        return {"nodes": list(self.nodes), "adjacency": adj}


# ---------------------------------------------------------------------------
# 查询缓存生命周期
# ---------------------------------------------------------------------------

@dataclass
class CacheEntry:
    key: str
    result: Any
    row_count: int
    byte_size: int
    created_at: float
    ttl_seconds: int
    hit_count: int = 0
    last_hit_at: float | None = None


class CacheManager:
    """按TTL和LRU管理查询结果缓存生命周期。"""

    def __init__(self, max_entries: int = 500, max_memory_bytes: int = 200 * 1024 * 1024):
        self._entries: dict[str, CacheEntry] = {}
        self._access_order: list[str] = []  # LRU: 最近访问在末尾
        self.max_entries = max_entries
        self.max_memory = max_memory_bytes

    def compute_key(self, query_desc: str, params: dict[str, Any]) -> str:
        raw = json.dumps({"query": query_desc, "params": params}, sort_keys=True)
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def get(self, key: str) -> Any | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if time.time() - entry.created_at > entry.ttl_seconds:
            self._evict(key)
            return None
        entry.hit_count += 1
        entry.last_hit_at = time.time()
        self._touch(key)
        return entry.result

    def put(
        self, key: str, result: Any, row_count: int = 0,
        ttl_seconds: int = 300, byte_size: int | None = None,
    ) -> None:
        if byte_size is None:
            byte_size = len(json.dumps(result, default=str).encode()) if result is not None else 0
        entry = CacheEntry(
            key=key, result=result, row_count=row_count,
            byte_size=byte_size, created_at=time.time(), ttl_seconds=ttl_seconds,
        )
        self._entries[key] = entry
        self._touch(key)
        self._enforce_limits()

    def invalidate(self, key: str) -> bool:
        return self._evict(key)

    def stats(self) -> dict[str, Any]:
        total_hits = sum(e.hit_count for e in self._entries.values())
        active = sum(1 for e in self._entries.values()
                     if time.time() - e.created_at <= e.ttl_seconds)
        total_mem = sum(e.byte_size for e in self._entries.values())
        return {
            "total": len(self._entries),
            "active": active,
            "expired": len(self._entries) - active,
            "total_hits": total_hits,
            "memory_bytes": total_mem,
        }

    def _touch(self, key: str) -> None:
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def _evict(self, key: str) -> bool:
        if key in self._entries:
            del self._entries[key]
        if key in self._access_order:
            self._access_order.remove(key)
        return True

    def _enforce_limits(self) -> None:
        while len(self._entries) > self.max_entries and self._access_order:
            lru_key = self._access_order[0]
            self._evict(lru_key)
        total_mem = sum(e.byte_size for e in self._entries.values())
        while total_mem > self.max_memory and self._access_order:
            lru_key = self._access_order[0]
            self._evict(lru_key)
            total_mem = sum(e.byte_size for e in self._entries.values())


# ---------------------------------------------------------------------------
# 异常检测
# ---------------------------------------------------------------------------

def detect_anomalies_zscore(
    values: list[float], threshold: float = 3.0
) -> list[dict[str, Any]]:
    """Z-Score 异常检测。"""
    n = len(values)
    if n < 3:
        return []
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std = math.sqrt(variance) if variance > 0 else 1e-10
    anomalies: list[dict[str, Any]] = []
    for i, v in enumerate(values):
        z = abs(v - mean) / std
        if z > threshold:
            anomalies.append({"index": i, "value": v, "z_score": round(z, 3), "method": "zscore"})
    return anomalies


def detect_anomalies_iqr(values: list[float], multiplier: float = 1.5) -> list[dict[str, Any]]:
    """IQR 异常检测。"""
    n = len(values)
    if n < 4:
        return []
    sorted_vals = sorted(values)
    q1_idx = n // 4
    q3_idx = (3 * n) // 4
    q1 = sorted_vals[q1_idx]
    q3 = sorted_vals[q3_idx]
    iqr = q3 - q1
    if iqr == 0:
        return []
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    anomalies: list[dict[str, Any]] = []
    for i, v in enumerate(values):
        if v < lower or v > upper:
            anomalies.append({"index": i, "value": v, "lower": lower, "upper": upper, "method": "iqr"})
    return anomalies


def detect_anomalies_moving_avg(
    values: list[float], window: int = 7, deviation_ratio: float = 0.3,
) -> list[dict[str, Any]]:
    """基于移动平均偏差的异常检测。"""
    n = len(values)
    if n < window:
        return []
    anomalies: list[dict[str, Any]] = []
    for i in range(window, n):
        window_vals = values[i - window:i]
        avg = sum(window_vals) / window
        if avg == 0:
            continue
        deviation = abs(values[i] - avg) / abs(avg)
        if deviation > deviation_ratio:
            anomalies.append({
                "index": i, "value": values[i], "moving_avg": round(avg, 3),
                "deviation_pct": round(deviation * 100, 2), "method": "moving_avg",
            })
    return anomalies


# ---------------------------------------------------------------------------
# A/B 实验统计
# ---------------------------------------------------------------------------

def compute_ab_sample_size(
    baseline_rate: float, min_detectable_effect: float,
    alpha: float = 0.05, power: float = 0.8,
) -> int:
    """计算A/B实验最小样本量。"""
    z_alpha = 1.96  # 双尾 alpha=0.05
    z_beta = 0.84   # power=0.8
    p = baseline_rate
    delta = min_detectable_effect
    n = (2 * (z_alpha + z_beta) ** 2 * p * (1 - p)) / (delta ** 2)
    return max(1, math.ceil(n))


def compute_confidence_interval(
    values_a: list[float], values_b: list[float], confidence: float = 0.95,
) -> dict[str, Any]:
    """计算两组数据的置信区间和均值差异。"""
    na, nb = len(values_a), len(values_b)
    if na < 2 or nb < 2:
        return {"error": "样本量不足"}

    mean_a = sum(values_a) / na
    mean_b = sum(values_b) / nb
    diff = mean_b - mean_a

    var_a = sum((v - mean_a) ** 2 for v in values_a) / (na - 1)
    var_b = sum((v - mean_b) ** 2 for v in values_b) / (nb - 1)

    se = math.sqrt(var_a / na + var_b / nb)
    if se == 0:
        return {"mean_a": mean_a, "mean_b": mean_b, "diff": diff, "significant": False, "reason": "零方差"}

    z_value = 1.96 if confidence == 0.95 else 2.576  # 简化

    ci_lower = diff - z_value * se
    ci_upper = diff + z_value * se
    significant = (ci_lower > 0 or ci_upper < 0)

    return {
        "mean_a": round(mean_a, 4),
        "mean_b": round(mean_b, 4),
        "diff": round(diff, 4),
        "diff_pct": round(diff / (abs(mean_a) if mean_a != 0 else 1) * 100, 2),
        "ci_lower": round(ci_lower, 4),
        "ci_upper": round(ci_upper, 4),
        "significant": significant,
        "confidence": confidence,
    }


# ---------------------------------------------------------------------------
# 下钻路径计算
# ---------------------------------------------------------------------------

def compute_drill_levels(
    data: list[dict[str, Any]],
    drill_hierarchy: list[str],  # e.g. ["region", "city", "store"]
    measures: list[str],
    aggregator: str = "SUM",
) -> list[dict[str, Any]]:
    """按层级分组计算各层度量值。"""
    results: list[dict[str, Any]] = []
    for level_idx, dim in enumerate(drill_hierarchy):
        group_map: dict[str, dict[str, list[float]]] = {}
        for row in data:
            key = str(row.get(dim, ""))
            if key not in group_map:
                group_map[key] = {m: [] for m in measures}
            for m in measures:
                try:
                    group_map[key][m].append(float(row.get(m, 0)))
                except (ValueError, TypeError):
                    pass
        level_data: dict[str, Any] = {
            "level": level_idx,
            "dimension": dim,
            "groups": [],
        }
        for key, mdata in group_map.items():
            entry = {"key": key}
            for m in measures:
                values = mdata.get(m, [])
                entry[m] = round(compute_metric(values, aggregator), 2)
            level_data["groups"].append(entry)
        results.append(level_data)
    return results


# ---------------------------------------------------------------------------
# 查询加速预计算策略
# ---------------------------------------------------------------------------

def recommend_materialization(
    query_patterns: list[dict[str, Any]],  # [{query_id, frequency, avg_latency_ms, row_count}]
) -> list[dict[str, Any]]:
    """根据查询模式推荐物化策略。"""
    recommendations: list[dict[str, Any]] = []
    for q in query_patterns:
        freq = int(q.get("frequency", 0))
        latency = float(q.get("avg_latency_ms", 0))
        rows = int(q.get("row_count", 0))

        score = freq * max(1, latency / 100) * max(1, rows / 1000)
        if score > 50:
            method = "物化视图" if rows > 10000 else "预计算" if rows > 1000 else "结果缓存"
            strategy = {
                "query_id": q.get("query_id", ""),
                "score": round(score, 2),
                "recommended_method": method,
                "priority": "高" if score > 500 else "中" if score > 200 else "低",
            }
            recommendations.append(strategy)
    return sorted(recommendations, key=lambda x: float(x["score"]), reverse=True)


# ---------------------------------------------------------------------------
# 导出任务调度器
# ---------------------------------------------------------------------------

class ExportScheduler:
    """异步导出任务调度器（基于优先级队列）。"""

    def __init__(self, max_concurrent: int = 3):
        self._queue: list[dict[str, Any]] = []
        self._running: set[str] = set()
        self.max_concurrent = max_concurrent

    def enqueue(
        self, task_id: str, priority: int = 0,
        sql: str = "", format: str = "csv",
    ) -> str:
        self._queue.append({
            "task_id": task_id,
            "priority": priority,
            "sql": sql,
            "format": format,
            "enqueued_at": time.time(),
        })
        self._queue.sort(key=lambda x: (-x["priority"], x["enqueued_at"]))
        return task_id

    def dequeue(self) -> dict[str, Any] | None:
        if not self._queue or len(self._running) >= self.max_concurrent:
            return None
        task = self._queue.pop(0)
        self._running.add(task["task_id"])
        return task

    def complete(self, task_id: str) -> None:
        self._running.discard(task_id)

    def queue_size(self) -> int:
        return len(self._queue)

    def running_count(self) -> int:
        return len(self._running)