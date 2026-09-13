/**
 * RCA → 拓扑的跨视图共享（W11-3 根因链路高亮的数据通路）。
 *
 * 为什么不放 URL / 不让拓扑视图自己再拉一次 GET /rca：
 *  - RCA 分析是「按请求重跑 + 每请求写审计」的同步端点（rest_rca.go），
 *    拓扑仅为高亮二次触发分析既浪费算力又污染审计；
 *  - 事件详情里跑过一次的结果顺手登记即可。
 * 模块级单例 + 订阅：路由切换（#/incidents → #/topology）不丢；页面刷新
 * 即清空——刻意不持久化，避免把分析快照变成前端第二真相（要新结果就重跑）。
 */
import { useSyncExternalStore } from "react";

export interface RcaHighlight {
  incidentId: string;
  title?: string;
  /** root_causes 各条 node_keys 的并集（拓扑红环节点集）。 */
  rootNodes: string[];
  at: number; // 登记时刻（Date.now），仅用于展示新鲜度
}

let current: RcaHighlight | null = null;
const subs = new Set<() => void>();

function emit(): void {
  for (const f of subs) f();
}

/** 事件详情跑完 RCA 后登记（root_causes 为空也登记——"没标出根因"同样是拓扑上要明示的事实）。 */
export function setRcaHighlight(h: RcaHighlight): void {
  current = h;
  emit();
}

export function clearRcaHighlight(): void {
  current = null;
  emit();
}

/** React 订阅入口（hash 路由下视图是条件渲染，钩子只在挂载视图内取用）。 */
export function useRcaHighlight(): RcaHighlight | null {
  return useSyncExternalStore(
    (cb) => { subs.add(cb); return () => { subs.delete(cb); }; },
    () => current,
    () => null,
  );
}
