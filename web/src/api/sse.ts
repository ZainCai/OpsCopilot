import { apiUrl } from "./base";
import { ENDPOINTS, SSE_EVENT_INCIDENT } from "./endpoints";
import type { SSEIncidentMessage } from "./types";

/**
 * SSE 封装（GET /api/v1/events/stream）。
 *
 * 断线重连策略（质量线要求）：
 *  - EventSource 原生对 CONNECTING/OPEN 抖动会自行重连；
 *  - 但连接进入 CLOSED（服务端显式关闭 / 代理掐断后耗尽重试）时不会自救，
 *    这里补一层带指数退避的手动重建（1s → 2s → 5s → 10s 封顶）；
 *  - onopen 成功后退避归零。
 * 调用方通过 onStatus 得到连接态，UI 据此展示"实时推送/重连中/轮询降级"。
 */
export type StreamStatus = "connecting" | "live" | "reconnecting" | "poll";

export interface IncidentStreamHandle {
  close(): void;
}

export function openIncidentStream(
  onIncident: (msg: SSEIncidentMessage) => void,
  onStatus: (s: StreamStatus) => void,
): IncidentStreamHandle {
  let es: EventSource | null = null;
  let closed = false;
  let attempt = 0;
  let retryTimer: ReturnType<typeof setTimeout> | null = null;

  const backoff = () => Math.min(1000 * 2 ** attempt, 10_000);

  function connect(): void {
    if (closed) return;
    if (typeof EventSource === "undefined") {
      onStatus("poll"); // 浏览器不支持：调用方降级轮询
      return;
    }
    onStatus(attempt === 0 ? "connecting" : "reconnecting");
    try {
      es = new EventSource(apiUrl(ENDPOINTS.eventsStream));
    } catch {
      es = null;
      onStatus("poll");
      return;
    }
    es.onopen = () => {
      attempt = 0;
      onStatus("live");
    };
    es.addEventListener(
      SSE_EVENT_INCIDENT,
      ((ev: MessageEvent<string>) => {
        try {
          onIncident(JSON.parse(ev.data) as SSEIncidentMessage);
        } catch {
          /* 脏消息忽略，不打断流 */
        }
      }) as EventListener,
    );
    es.onerror = () => {
      if (closed || !es) return;
      if (es.readyState === EventSource.CLOSED) {
        // 原生重连已耗尽：退避后手动重建
        es.close();
        es = null;
        attempt += 1;
        onStatus("reconnecting");
        retryTimer = setTimeout(connect, backoff());
      } else {
        // 仍是 CONNECTING：EventSource 自己重连，UI 先按"重连中"呈现，
        // 同时通知调用方可临时开启轮询兜底，避免"卡死不更新"。
        onStatus("poll");
      }
    };
  }

  connect();

  return {
    close() {
      closed = true;
      if (retryTimer !== null) clearTimeout(retryTimer);
      if (es) {
        es.close();
        es = null;
      }
    },
  };
}
