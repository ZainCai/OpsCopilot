// 双分辨率走查工具（W12 工具链评估落地）：puppeteer-core + 系统 Edge，
// 无需下载浏览器。用法：node scripts/walkthrough.mjs [baseUrl]
// 产出：docs/reviews/web-walkthrough-<yyyymmdd>/ 下截图 PNG + report.json
// （每视口×路由的侧栏宽度、横向溢出、控制台错误读数）。
import { mkdirSync, writeFileSync } from "node:fs";
import puppeteer from "puppeteer-core";

const BASE = process.argv[2] || "http://[::1]:5173";
const EDGE = "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe";
const DAY = new Date().toISOString().slice(0, 10).replaceAll("-", "");
const OUT = `../docs/reviews/web-walkthrough-${DAY}`;
mkdirSync(OUT, { recursive: true });

const VIEWPORTS = [
  { name: "1920", w: 1920, h: 1080 },
  { name: "1366", w: 1366, h: 768 },
  { name: "1180-rail", w: 1180, h: 800 },   // <1200 侧栏应塌成 64px 图标轨
  { name: "900", w: 900, h: 800 },          // kpi 两列
  { name: "390-mobile", w: 390, h: 844 },   // kpi 单列
];
const ROUTES = ["overview", "alerts", "incidents", "topology", "settings", "audit"];

const browser = await puppeteer.launch({
  executablePath: EDGE, headless: "new",
  args: ["--no-sandbox", "--disable-gpu", "--window-size=1920,1080"],
});
const report = { base: BASE, day: DAY, results: [] };

for (const vp of VIEWPORTS) {
  const page = await browser.newPage();
  await page.setViewport({ width: vp.w, height: vp.h });
  // 预置写路径 token/操作人（灰度实例 GRAY_TOKEN 默认 dev）
  await page.evaluateOnNewDocument(() => {
    sessionStorage.setItem("ops_token", "dev");
    sessionStorage.setItem("ops_actor", "walkthrough");
  });
  const errs = [];
  page.on("pageerror", (e) => errs.push("pageerror: " + e.message));
  // 网络 4xx/5xx 噪音不直收 console 文本；用 response 监听分类：
  // /rca/session 与 /rca 的 503 是设计内降级（OPS_SESSION/OPS_RCA off 时抽屉/面板自隐藏），记 expected 不计错。
  page.on("response", (r) => {
    const u = r.url();
    if (!u.includes("/api/")) return;
    const s = r.status();
    if (s >= 500 && /\/rca(\/session)?$/.test(u)) return; // 降级探针，预期
    if (s >= 400) errs.push(`http ${s}: ${u.replace(BASE, "")}`);
  });

  for (const route of ROUTES) {
    await page.goto(`${BASE}/#/${route}`, { waitUntil: "domcontentloaded", timeout: 30000 });
    // 等首屏数据落版（SSE 常连会拖累 networkidle；以表格行/空态出现为准 + 400ms 稳定）
    await page.waitForSelector(".tbl tbody tr, .empty", { timeout: 8000 }).catch(() => {});
    await new Promise((r) => setTimeout(r, 400));
    if (route === "incidents") {
      // 打开首行详情（RCA/时间线/Runbook 双栏布局的验收面）
      const clicked = await page.evaluate(() => {
        const row = document.querySelector(".tbl tbody tr.rowlink") ?? document.querySelector(".tbl tbody tr");
        if (row) { row.click(); return true; }
        return false;
      });
      if (clicked) {
        await page.waitForSelector(".inc-page", { timeout: 8000 }).catch(() => {});
        await new Promise((r) => setTimeout(r, 400));
      }
    }
    const metrics = await page.evaluate(() => {
      const sb = document.querySelector(".sidebar");
      const ng = document.querySelector(".nav-group");
      return {
        sidebarW: sb ? Math.round(sb.getBoundingClientRect().width) : -1,
        navGroupVisible: ng ? getComputedStyle(ng).display !== "none" : false,
        hScroll: document.documentElement.scrollWidth - document.documentElement.clientWidth,
      };
    });
    const shot = `${vp.name}-${route}.png`;
    await page.screenshot({ path: `${OUT}/${shot}` });
    report.results.push({ viewport: vp.name, route, ...metrics, shot, errors: errs.splice(0) });
  }
  await page.close();
}
await browser.close();

mkdirSync(OUT, { recursive: true });
writeFileSync(`${OUT}/report.json`, JSON.stringify(report, null, 2));
const bad = report.results.filter((r) => r.hScroll > 2 || r.errors.length > 0);
console.log(`pages=${report.results.length} overflowOrError=${bad.length}`);
for (const b of bad) console.log("  !", b.viewport, b.route, `hScroll=${b.hScroll}`, b.errors.slice(0, 2).join(" | "));
