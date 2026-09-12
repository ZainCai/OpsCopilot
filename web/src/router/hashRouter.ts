import { useCallback, useEffect, useState } from "react";

/**
 * 手写 hash 路由（少依赖优先——不引 react-router，见 ADR-013）。
 * 形如 #/alerts、#/incidents；深链与内嵌 console 的 ?view= 习惯对齐。
 */
export interface Route {
  path: string; // "alerts" | "incidents" | ...
  query: URLSearchParams;
}

function parseHash(): Route {
  const raw = window.location.hash.replace(/^#\/?/, "");
  const [path, qs] = raw.split("?");
  return { path: path || "overview", query: new URLSearchParams(qs ?? "") };
}

export function navigate(path: string): void {
  window.location.hash = `#/${path}`;
}

export function useHashRoute(): Route {
  const [route, setRoute] = useState<Route>(parseHash);
  const onChange = useCallback(() => setRoute(parseHash()), []);
  useEffect(() => {
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, [onChange]);
  return route;
}
