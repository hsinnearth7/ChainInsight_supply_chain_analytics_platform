import pandas as pd
import numpy as np
import random

# 設定隨機種子以確保結果可重現
np.random.seed(42)
random.seed(42)

# 1. 參數設定
TARGET_ROWS = 10000
categories = ['Electronics', 'Home', 'Food', 'Shipping', 'Office', 'Apparel', 'Industrial']
vendors = ['Tokyo Electronics', 'Fukuoka Logistics', 'Hokkaido Foods', 'Kyoto Crafts', 
           'Osaka Supplies', 'Nagoya Parts', 'Sapporo Steel']

data = []

# 2. 輔助函數：製造髒數據
def make_dirty_cost(val):
    r = random.random()
    if r < 0.05: return None # 缺失值
    if r < 0.10: return f"${val:.2f}" # 帶幣別符號
    if r < 0.15: return f"USD {val:.2f}" # 帶文字
    if val > 1000 and r < 0.20: return f"{val:,.2f}" # 帶逗號 (例如 1,200.00)
    if r < 0.22: return "Quote Pending" # 純文字垃圾
    return val # 正常數值

def make_dirty_stock(val):
    r = random.random()
    if r < 0.03: return -val # 負庫存 (邏輯錯誤)
    if r < 0.06: return None # 缺失值
    if r < 0.10: return f"{val} pcs" # 帶單位
    return val

# 3. 生成 10,000 筆數據
for i in range(TARGET_ROWS):
    # 生成模擬 SKU
    sku = f"SKU-{random.choice(['A','B','C','X','Y','Z'])}{random.randint(1000, 9999)}"
    cat = random.choice(categories)
    vendor = random.choice(vendors)
    
    # 基礎數值
    cost = round(random.uniform(5, 500), 2)
    demand = random.randint(1, 100) # 日均銷量
    safety_stock = int(demand * random.uniform(7, 14)) # 安全庫存
    stock = int(demand * random.uniform(0, 60)) # 當前庫存 (0~60天水位)
    lead_time = random.randint(3, 30) # 交期
    
    # --- 注入髒數據邏輯 ---
    
    # 類別大小寫混亂 & 拼字錯誤
    if random.random() < 0.1:
        cat = cat.lower() if random.random() < 0.5 else cat.upper()
    
    # SKU 前後空白 (Trim 問題)
    if random.random() < 0.05:
        sku = f" {sku} "
        
    row = {
        'Product_ID': sku,
        'Category': cat,
        'Unit_Cost_Raw': make_dirty_cost(cost),   # 髒成本欄位
        'Current_Stock_Raw': make_dirty_stock(stock), # 髒庫存欄位
        'Daily_Demand_Est': demand if random.random() > 0.05 else np.nan, # 5% 缺失銷量
        'Safety_Stock_Target': safety_stock,
        'Vendor_Name': vendor,
        'Lead_Time_Days': lead_time
    }
    data.append(row)

df_dirty = pd.DataFrame(data)

# 加入重複值 (Duplicate Rows)
df_dirty = pd.concat([df_dirty, df_dirty.sample(50)], ignore_index=True)

# 匯出 CSV
df_dirty.to_csv('Supply_Chain_Inventory_Dirty_10k.csv', index=False)
print(f"已生成 {len(df_dirty)} 筆髒數據，請開始清洗！")
print(df_dirty.head())
