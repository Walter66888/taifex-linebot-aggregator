# 在 server.py 中，修改 build_report 函数来从正确的数据源获取数据

def safe_latest(prod: str) -> str:
    """把 None / 空 dict 轉成 '–'，並加上千分位"""
    doc = fut_latest(prod)  # 使用 fut_contracts.py 中定义的 latest 函数
    logging.info(f"產品 {prod} 的資料: {doc}")
    
    if not doc:
        logging.warning(f"無法找到產品 {prod} 的資料")
        return "–"
        
    val = doc.get("retail_net")
    logging.info(f"產品 {prod} 的 retail_net: {val}")
    return f"{val:+,}" if val is not None else "–"

def build_report() -> str:
    """建立報告，包含 PC ratio 和散戶小台未平倉"""
    # 使用台灣時區
    tw_tz = timezone(timedelta(hours=8))
    
    # 獲取 PC ratio
    pc_data = pc_latest()
    pc_ratio = "–"
    if pc_data and isinstance(pc_data, dict):
        pc_ratio = f"{pc_data.get('pc_oi_ratio', '–'):.2f}"
    
    # 獲取最新期貨資料
    mtx_data = safe_latest('mtx')
    imtx_data = safe_latest('imtx')
    
    # 當前日期格式化
    today = datetime.now(tw_tz).strftime("%Y/%m/%d (%a)")
    
    # 構建報告
    return (
        f"日期：{today}\n"
        f"🧮 PC ratio 未平倉比：{pc_ratio}\n\n"
        f"💼 散戶未平倉（口數）\n"
        f"小台：{mtx_data}\n"
        f"微台：{imtx_data}"
    )

# 添加一個診斷路由
@app.route("/debug_html", methods=["GET"])
def debug_html():
    """查看最新的原始 HTML 內容（需要管理员权限）"""
    token = request.args.get("token", "")
    if token not in ADMIN_USER_IDS:
        abort(403)
    
    from crawler.fut_contracts import get_raw_html
    html = get_raw_html()
    if not html:
        return "No HTML data found", 404
    
    return html

# 診斷資料路由
@app.route("/debug_data", methods=["GET"])
def debug_data():
    """查看解析後的期貨資料（需要管理员权限）"""
    token = request.args.get("token", "")
    if token not in ADMIN_USER_IDS:
        abort(403)
    
    from crawler.fut_contracts import latest
    
    mtx = latest("mtx")
    imtx = latest("imtx")
    
    return jsonify({
        "mtx": mtx,
        "imtx": imtx
    })
