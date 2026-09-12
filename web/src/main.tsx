import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./styles/tokens.css";
import "./styles/base.css";

const el = document.getElementById("root");
if (!el) throw new Error("#root 缺失——index.html 与工程不同步？");
createRoot(el).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
