# Taifex Line Bot & Crawlers

自動抓取 **台指選擇權 Put/Call 比** 與 **小型臺指期貨三大法人淨額**，寫入 MongoDB，並提供 Line Bot 查詢。

## 🚀 部署流程

1. **Fork / push** 此 repo 至 GitHub。
2. **Settings → Secrets → Actions** 新增：
   * `MONGODB_URI`：`mongodb+srv://...` (read‑write)
   * `LINE_CHANNEL_SECRET`、`LINE_CHANNEL_ACCESS_TOKEN`（僅 bot 需要）
3. Actions → *crawl-taifex-seed* → Run workflow → 輸入 `start=2024-01-01` 回填。
4. Actions → *crawl-taifex-schedule* 自動於台灣 15:05 起重試抓取直到更新成功。
5. Render 建立 Web Service：
   * Start Command：`gunicorn bot.handlers:app`
   * Environment：同 Secrets。
6. Line Developers Webhook 設定 `https://<render>.onrender.com/callback`。

## 📜 指令

* `/today` — 回傳今日 PC ratio 與散戶小台未平倉
