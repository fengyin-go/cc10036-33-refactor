"""同期群分析领域服务：保留率计算、群组追踪、跨模块数据消费。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.analytics_engine import EventDecodeError, compute_cohort_retention


class CohortAnalysisService(BaseModuleService):
    MODULE = "cohort_analysis"
    ENTITY = "群组配置"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["群组名称", "初始事件", "回访事件", "分析周期", "群组大小", "保留计算", "显示格式"]
    REQUIRED_FIELDS = ["群组名称", "初始事件", "回访事件"]
    STATUS_ORDER = ["草稿", "分析中", "已完成", "已归档"]
    ACTION_RULES = {"开始分析": "分析中", "完成分析": "已完成", "归档群组": "已归档"}
    NEGATIVE_ACTIONS = []

    # ---- 业务方法 ----

    def run_cohort_analysis(
        self, config_id: int, events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """执行同期群分析：基于事件数据计算保留率矩阵。"""
        cohort_config = store.find(self.MODULE, config_id)
        if cohort_config is None:
            return {"ok": False, "message": f"同期群配置 {config_id} 不存在"}

        init_event = str(cohort_config.get("初始事件", ""))
        return_event = str(cohort_config.get("回访事件", ""))
        period_str = str(cohort_config.get("分析周期", "7"))
        try:
            period_days = int(period_str)
        except ValueError:
            period_days = 7

        max_periods = 8
        retention_method = str(cohort_config.get("保留计算", "standard")).lower()

        if retention_method == "bounded":
            max_periods = 12  # 有界保留

        try:
            matrix = compute_cohort_retention(
                events, init_event, return_event, period_days, max_periods,
            )
        except EventDecodeError as exc:
            # 计算失败：不推进状态、不写半成品；调用方可持原始批次复跑，
            # 失败批次的分子分母不会混入后续成果。
            return {
                "ok": False,
                "message": f"事件解码失败：{exc}",
                "failed_index": exc.index,
                "config_name": str(cohort_config.get("群组名称", "")),
            }

        self.run_action(config_id, "完成分析")

        return {
            "ok": True,
            "config_name": str(cohort_config.get("群组名称", "")),
            "init_event": init_event,
            "return_event": return_event,
            "period_days": period_days,
            "max_periods": max_periods,
            "cohort_count": len(matrix.cohort_ids),
            "cohort_ids": matrix.cohort_ids,
            "period_labels": matrix.period_labels,
            "retention_matrix": matrix.retention_matrix,
            "avg_retention_week1": round(
                sum(row[0] for row in matrix.retention_matrix if len(row) > 0)
                / max(len(matrix.retention_matrix), 1), 2,
            ),
        }

    def compare_cohort_retention(
        self, config_id_1: int, config_id_2: int,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """比较两个群组的保留率。"""
        result_1 = self.run_cohort_analysis(config_id_1, events)
        result_2 = self.run_cohort_analysis(config_id_2, events)
        if not result_1.get("ok") or not result_2.get("ok"):
            return {"ok": False, "message": "分析运行失败"}
        r1_avg = result_1.get("avg_retention_week1", 0)
        r2_avg = result_2.get("avg_retention_week1", 0)
        return {
            "ok": True,
            "config_1": result_1.get("config_name"),
            "config_2": result_2.get("config_name"),
            "retention_1_week1": r1_avg,
            "retention_2_week1": r2_avg,
            "difference": round(r2_avg - r1_avg, 2),
        }

    def get_retention_for_segment(
        self, segment_id: str, events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """按分群筛选事件后运行同期群分析（分群分析→同期群分析的跨模块消费）。"""
        from app.services import service_for
        segment_svc = service_for("segment_rule")
        if segment_svc is None:
            return {"ok": False, "message": "分群服务不可用"}
        segment = store.find("segment_rule", int(segment_id))
        if segment is None:
            return {"ok": False, "message": f"分群 {segment_id} 不存在"}
        seg_results = segment_svc.compute_segment(int(segment_id), events)
        matched_data = seg_results.get("sample", events)
        return self.run_cohort_analysis(1, matched_data)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["artifact"] = "同期群分析数据集"
        rows = store.rows(self.MODULE)
        base["completed_analyses"] = sum(1 for row in rows if row.get("status") == "已完成")
        return base