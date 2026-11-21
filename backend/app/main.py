"""多维分析查询与指标平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.store import store
from app.services import service_for
from app.analytics_engine import events_fingerprint

app = FastAPI(title="多维分析查询与指标平台", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# ============ 跨模块分析链（按5条主要数据流设计） ============


# ---- 数据流1: 指标定义 → 语义层映射 → 多维立方构建 → 切片查询 ----
@app.post("/api/analytics/metric-pipeline")
def metric_pipeline(metric_code: str = "DAU", sample_data: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """完整指标体系流水线：指标口径解析→语义层映射→立方预计算→切片查询执行。"""
    metric_svc = service_for("metric_def")
    semantic_svc = service_for("semantic_layer")
    cube_svc = service_for("cube_builder")
    slice_svc = service_for("slice_query")

    if any(s is None for s in [metric_svc, semantic_svc, cube_svc, slice_svc]):
        return {"ok": False, "message": "依赖服务未就绪"}

    # Step 1: 指标口径解析
    caliber = metric_svc.resolve_metric_caliber(metric_code)
    if caliber is None:
        return {"ok": False, "message": f"指标 {metric_code} 未定义"}

    # Step 2: 语义层映射
    mapping = semantic_svc.map_metric_to_physical(metric_code)

    # Step 3: 立方预计算（使用已上线立方）
    online_cubes = cube_svc.get_online_cubes()
    cube_results: list[dict[str, Any]] = []
    if sample_data is None:
        sample_data = _sample_behavior_data()
    for cube in online_cubes[:2]:
        precompute = cube_svc.precompute_cube(int(cube["id"]), sample_data)
        cube_results.append(precompute)

    # Step 4: 切片查询
    slice_result = None
    published_templates = [
        t for t in store.rows("slice_query") if t.get("status") == "已发布"
    ]
    if published_templates:
        slice_result = slice_svc.execute_slice_query(int(published_templates[0]["id"]), sample_data)

    return {
        "ok": True,
        "pipeline": "指标定义→语义层→立方构建→切片查询",
        "metric_caliber": caliber,
        "physical_mapping": mapping,
        "cube_precompute": cube_results[:2],
        "slice_query": slice_result,
    }


# ---- 数据流2: 分群规则 → 同期群分析 → 归因模型 → 结果缓存 ----
@app.post("/api/analytics/segment-pipeline")
def segment_pipeline(
    rule_id: int = 1, events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """分群分析流水线：分群计算→同期群保留率→归因权重→缓存存储。"""
    segment_svc = service_for("segment_rule")
    cohort_svc = service_for("cohort_analysis")
    attribution_svc = service_for("attribution_model")
    cache_svc = service_for("result_cache")

    if any(s is None for s in [segment_svc, cohort_svc, attribution_svc, cache_svc]):
        return {"ok": False, "message": "依赖服务未就绪"}

    if events is None:
        events = _sample_user_events()

    # Step 1: 分群计算
    segment_result = segment_svc.compute_segment(rule_id, events)

    # Step 2: 同期群分析
    cohort_result = cohort_svc.run_cohort_analysis(1, events)

    # Step 3: 归因模型
    active_models = attribution_svc.get_active_models()
    attribution_results: list[dict[str, Any]] = []
    for model in active_models[:2]:
        result = attribution_svc.compute_attribution_weights(int(model["id"]), _sample_touchpoints())
        attribution_results.append(result)

    # Step 4: 缓存结果（缓存键含原始批次指纹：复跑同一批次才命中；
    # 计算失败的批次不写入，避免两个批次的分子分母混进同一份成果）
    cached = False
    if cohort_result.get("ok"):
        _ = cache_svc.get_or_compute(
            "segment_pipeline",
            {"rule_id": rule_id, "events_fingerprint": events_fingerprint(events)},
            compute_fn=lambda: {"segment": segment_result.get("matched_count"), "cohort": cohort_result},
            ttl_seconds=600,
        )
        cached = True

    return {
        "ok": True,
        "pipeline": "分群圈选→同期群分析→归因模型→结果缓存",
        "segment": segment_result,
        "cohort": cohort_result,
        "attribution": attribution_results,
        "cached": cached,
    }


# ---- 数据流3: 看板布局 → 即席导出 → 报表订阅 → 查询加速 ----
@app.post("/api/analytics/dashboard-pipeline")
def dashboard_pipeline(
    dashboard_id: int = 1, query_data: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """看板导出流水线：看板布局→即席导出→报表订阅→查询加速。"""
    dashboard_svc = service_for("dashboard_layout")
    export_svc = service_for("adhoc_export")
    sub_svc = service_for("report_subscription")
    accelerator_svc = service_for("query_accelerator")

    if any(s is None for s in [dashboard_svc, export_svc, sub_svc, accelerator_svc]):
        return {"ok": False, "message": "依赖服务未就绪"}

    if query_data is None:
        query_data = _sample_behavior_data()

    # Step 1: 看板布局解析
    layout = dashboard_svc.parse_layout(dashboard_id)

    # Step 2: 即席导出
    export_result = export_svc.submit_export(1, query_data)

    # Step 3: 报表订阅
    dash_name = layout.get("dashboard_name", "")
    subscriptions = sub_svc.get_subscriptions_for_dashboard(dash_name)

    # Step 4: 查询加速
    cache_hit = accelerator_svc.compute_cache_hit_rate()

    return {
        "ok": True,
        "pipeline": "看板布局→即席导出→报表订阅→查询加速",
        "layout": layout,
        "export": export_result,
        "subscription_count": len(subscriptions),
        "accelerator_hit_rate": cache_hit.get("hit_rate_pct"),
    }


# ---- 数据流4: 异常检测 → 实验分析 → AB指标 ----
@app.post("/api/analytics/monitor-pipeline")
def monitor_pipeline(
    metric_values: list[float] | None = None,
) -> dict[str, Any]:
    """监控实验流水线：异常检测→实验分析→AB置信区间。"""
    anomaly_svc = service_for("anomaly_detect")
    ab_svc = service_for("ab_analysis")

    if any(s is None for s in [anomaly_svc, ab_svc]):
        return {"ok": False, "message": "依赖服务未就绪"}

    if metric_values is None:
        metric_values = _sample_metric_timeseries()

    # Step 1: 异常检测
    anomaly_result = anomaly_svc.detect(1, metric_values)

    # Step 2: 实验分析（模拟对照组/实验组数据）
    half = len(metric_values) // 2
    control = metric_values[:half]
    treatment = [v * 1.05 for v in metric_values[half:]]  # +5%效果
    ab_result = ab_svc.analyze(1, control, treatment)

    # Step 3: 实验期间异常检测联动
    exp_anomalies = ab_svc.check_anomalies_during_experiment(2, metric_values)

    return {
        "ok": True,
        "pipeline": "异常检测→实验分析→AB指标",
        "anomaly_detection": anomaly_result,
        "ab_analysis": ab_result,
        "experiment_anomalies": exp_anomalies,
    }


# ---- 数据流5: 数据血缘 → 数据范围 → 查询调度 → 下钻路径 ----
@app.post("/api/analytics/governance-pipeline")
def governance_pipeline(
    node_id: str = "1", metric_values: list[float] | None = None,
) -> dict[str, Any]:
    """数据治理流水线：血缘图→数据范围→调度拓扑→下钻分析。"""
    lineage_svc = service_for("data_lineage")
    scope_svc = service_for("olap_scope")
    scheduler_svc = service_for("query_scheduler")
    drill_svc = service_for("drill_path")

    if any(s is None for s in [lineage_svc, scope_svc, scheduler_svc, drill_svc]):
        return {"ok": False, "message": "依赖服务未就绪"}

    # Step 1: 血缘下游影响分析
    impact = lineage_svc.compute_downstream(node_id)

    # Step 2: 数据范围检查
    scope_check = scope_svc.check_access("分析师A", "cube_user_behavior")

    # Step 3: 调度拓扑排序
    topo = scheduler_svc.topological_order()

    # Step 4: 下钻路径
    if metric_values is None:
        metric_values = _sample_metric_timeseries()
    sample_data = [{"region": "华东", "city": "上海", "store": f"门店{i}", "revenue": v} for i, v in enumerate(metric_values[:10])]
    drill_result = drill_svc.execute_drill(1, sample_data)

    return {
        "ok": True,
        "pipeline": "数据血缘→数据范围→查询调度→下钻路径",
        "lineage_impact": impact,
        "scope_check": scope_check,
        "topological_order": topo,
        "drill_result": drill_result,
    }


# ----  天平 ----
@app.get("/api/analytics/lineage/{node_id}/upstream")
def lineage_upstream(node_id: str) -> dict[str, Any]:
    """递归计算血缘上游链（图结构计算）。"""
    svc = service_for("data_lineage")
    if svc is None:
        return {"ok": False, "message": "血缘服务未就绪"}
    return svc.compute_upstream(node_id)


@app.get("/api/analytics/lineage/{node_id}/impact")
def lineage_impact(node_id: str) -> dict[str, Any]:
    """变更影响分析。"""
    svc = service_for("data_lineage")
    if svc is None:
        return {"ok": False, "message": "血缘服务未就绪"}
    return svc.impact_analysis(node_id)


@app.get("/api/analytics/cache/stats")
def cache_stats() -> dict[str, Any]:
    """缓存周期指标。"""
    svc = service_for("result_cache")
    if svc is None:
        return {"ok": False, "message": "缓存服务未就绪"}
    return svc.cache_stats()


@app.get("/api/analytics/scheduler/topology")
def scheduler_topology() -> dict[str, Any]:
    """调度依赖拓扑排序。"""
    svc = service_for("query_scheduler")
    if svc is None:
        return {"ok": False, "message": "调度服务未就绪"}
    return svc.topological_order()


# ---- 辅助数据生成函数 ----
def _sample_behavior_data() -> list[dict[str, Any]]:
    return [
        {"user_id": "u1", "event_name": "页面访问", "event_time": 1698768000 + i * 3600,
         "city": "北京" if i % 3 == 0 else "上海", "device": "ios" if i % 2 == 0 else "android",
         "amount": 100 + i * 10} for i in range(50)
    ]


def _sample_user_events() -> list[dict[str, Any]]:
    import random
    random.seed(42)
    events: list[dict[str, Any]] = []
    for uid in range(200):
        for day in range(30):
            if random.random() > 0.7:
                events.append({
                    "user_id": f"u{uid}",
                    "event_name": "注册" if day == 0 else "登录",
                    "event_time": 1698768000 + day * 86400 + random.randint(0, 86400),
                })
    return events


def _sample_touchpoints() -> list[dict[str, Any]]:
    return [
        {"name": "搜索广告", "time": 1698768000},
        {"name": "信息流广告", "time": 1698796800},
        {"name": "邮件营销", "time": 1698832800},
        {"name": "社交媒体", "time": 1698868800},
        {"name": "直接访问", "time": 1698883200},
    ]


def _sample_metric_timeseries() -> list[float]:
    import random, math
    random.seed(42)
    base = 100.0
    values: list[float] = []
    for i in range(100):
        trend = base + i * 0.5 + 10 * math.sin(i / 10)
        noise = random.gauss(0, 5)
        if i in (20, 45, 78):
            noise += random.choice([-30, 40])  # 异常点
        values.append(round(max(0, trend + noise), 2))
    return values


# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)