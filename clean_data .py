import pandas as pd
import numpy as np
import re

def clean_supply_chain_data(input_file, output_file):
    """
    完整的供應鏈庫存數據清洗 ETL 流程
    
    參數:
        input_file: 輸入的髒數據 CSV 文件路徑
        output_file: 輸出的乾淨數據 CSV 文件路徑
    """
    
    print("=== 開始數據清洗流程 ===\n")
    
    # ===== EXTRACT (提取) =====
    print("1. 載入數據...")
    df = pd.read_csv(input_file)
    print(f"   原始數據: {len(df)} 行, {len(df.columns)} 列")
    print(f"   欄位: {list(df.columns)}\n")
    
    # ===== TRANSFORM (轉換) =====
    print("2. 開始數據清洗...\n")
    
    # 2.1 清理 Product_ID - 去除前後空格
    print("   2.1 清理 Product_ID (去除空格)...")
    before_count = df['Product_ID'].str.len().sum()
    df['Product_ID'] = df['Product_ID'].str.strip()
    after_count = df['Product_ID'].str.len().sum()
    spaces_removed = before_count - after_count
    print(f"       ✓ 已清理 {len(df)} 個產品ID，移除 {spaces_removed} 個多餘空格")
    
    # 2.2 清理 Category - 統一首字母大寫
    print("   2.2 清理 Category (統一格式)...")
    unique_categories_before = df['Category'].nunique()
    df['Category'] = df['Category'].str.strip().str.capitalize()
    unique_categories_after = df['Category'].nunique()
    print(f"       ✓ 類別統一完成: {unique_categories_before} → {unique_categories_after} 個不同類別")
    print(f"       ✓ 類別清單: {', '.join(sorted(df['Category'].unique()))}")
    
    # 2.3 清理 Unit_Cost_Raw - 轉換為數值
    print("   2.3 清理 Unit_Cost_Raw (提取數值)...")
    print("       → 正在解析不同格式的價格 (USD, $, 純數字, Quote Pending 等)...")
    def clean_cost(value):
        """清理成本數據，提取數值"""
        if pd.isna(value):
            return np.nan
        
        # 轉換為字符串
        value_str = str(value).strip()
        
        # 如果是 "Quote Pending" 或其他非數值，返回 NaN
        if not any(char.isdigit() for char in value_str):
            return np.nan
        
        # 去除 "USD", "$", 逗號和空格，只保留數字和小數點
        cleaned = re.sub(r'[^\d.]', '', value_str)
        
        try:
            return float(cleaned)
        except ValueError:
            return np.nan
    
    df['Unit_Cost'] = df['Unit_Cost_Raw'].apply(clean_cost)
    invalid_costs = df['Unit_Cost'].isna().sum()
    valid_costs = len(df) - invalid_costs
    print(f"       ✓ 成功轉換 {valid_costs} 個價格，{invalid_costs} 個無效價格將被填充")
    
    # 2.4 清理 Current_Stock_Raw - 處理負數和空值
    print("   2.4 清理 Current_Stock_Raw (處理異常值)...")
    df['Current_Stock'] = pd.to_numeric(df['Current_Stock_Raw'], errors='coerce')
    
    # 將負數庫存設為 0 (負數庫存不合理)
    negative_stock_count = (df['Current_Stock'] < 0).sum()
    df.loc[df['Current_Stock'] < 0, 'Current_Stock'] = 0
    print(f"       ✓ 發現並修正 {negative_stock_count} 個負數庫存 → 設為 0")
    
    # 2.5 處理空值
    print("   2.5 處理空值...")
    
    # 對於 Current_Stock 的空值，可以用 0 或中位數填充
    # 這裡使用 0（表示缺貨）
    null_stock_count = df['Current_Stock'].isna().sum()
    df['Current_Stock'] = df['Current_Stock'].fillna(0)
    print(f"       ✓ 填充 {null_stock_count} 個空值庫存 → 設為 0 (缺貨)")
    
    # 對於 Unit_Cost 的空值，可以用同類別的平均值填充
    print("       → 正在用各類別中位數填充價格空值...")
    null_cost_count = df['Unit_Cost'].isna().sum()
    df['Unit_Cost'] = df.groupby('Category')['Unit_Cost'].transform(
        lambda x: x.fillna(x.median())
    )
    
    # 如果某個類別全部是空值，用全局中位數填充
    df['Unit_Cost'] = df['Unit_Cost'].fillna(df['Unit_Cost'].median())
    print(f"       ✓ 填充 {null_cost_count} 個空值價格 (使用類別中位數)")
    
    # 2.6 清理 Vendor_Name - 去除空格
    print("   2.6 清理 Vendor_Name...")
    unique_vendors = df['Vendor_Name'].nunique()
    df['Vendor_Name'] = df['Vendor_Name'].str.strip()
    print(f"       ✓ 清理完成，共 {unique_vendors} 個不同供應商")
    
    # 2.7 數據驗證
    print("   2.7 數據驗證...")
    # 確保所有數值列都是正數
    df['Daily_Demand_Est'] = df['Daily_Demand_Est'].clip(lower=0)
    df['Safety_Stock_Target'] = df['Safety_Stock_Target'].clip(lower=0)
    before_lead_time = (df['Lead_Time_Days'] < 1).sum()
    df['Lead_Time_Days'] = df['Lead_Time_Days'].clip(lower=1)  # 交貨時間至少1天
    print(f"       ✓ 數值欄位驗證完成")
    if before_lead_time > 0:
        print(f"       ✓ 修正 {before_lead_time} 個無效交貨時間 → 最少1天")
    
    # 2.8 添加衍生欄位（可選）
    print("   2.8 添加計算欄位...")
    # 計算再訂購點 (Reorder Point) = 日需求 × 交貨時間 + 安全庫存
    print("       → 計算再訂購點 (Reorder Point)...")
    df['Reorder_Point'] = (df['Daily_Demand_Est'] * df['Lead_Time_Days'] + 
                           df['Safety_Stock_Target'])
    
    # 計算庫存狀態
    print("       → 評估庫存狀態 (Out of Stock / Low Stock / Normal Stock)...")
    df['Stock_Status'] = df.apply(
        lambda row: 'Out of Stock' if row['Current_Stock'] == 0
        else 'Low Stock' if row['Current_Stock'] < row['Reorder_Point']
        else 'Normal Stock',
        axis=1
    )
    
    # 計算庫存價值
    print("       → 計算庫存總價值...")
    df['Inventory_Value'] = df['Current_Stock'] * df['Unit_Cost']
    
    # 統計庫存狀態
    out_of_stock = (df['Stock_Status'] == 'Out of Stock').sum()
    low_stock = (df['Stock_Status'] == 'Low Stock').sum()
    normal_stock = (df['Stock_Status'] == 'Normal Stock').sum()
    print(f"       ✓ 庫存狀態統計:")
    print(f"         - 缺貨: {out_of_stock} 個產品")
    print(f"         - 低庫存: {low_stock} 個產品")
    print(f"         - 正常: {normal_stock} 個產品")
    
    # ===== LOAD (載入) =====
    print("\n3. 儲存清洗後的數據...")
    
    # 選擇要輸出的欄位（包含原始和清洗後的）
    output_columns = [
        'Product_ID', 'Category', 'Unit_Cost', 'Current_Stock',
        'Daily_Demand_Est', 'Safety_Stock_Target', 'Vendor_Name',
        'Lead_Time_Days', 'Reorder_Point', 'Stock_Status', 'Inventory_Value'
    ]
    
    print(f"   → 準備輸出 {len(output_columns)} 個欄位...")
    df_clean = df[output_columns]
    
    print(f"   → 正在寫入 CSV 文件: {output_file}")
    df_clean.to_csv(output_file, index=False)
    
    print(f"   ✓ 清洗後數據已成功儲存至: {output_file}")
    print(f"   ✓ 清洗後數據: {len(df_clean)} 行, {len(df_clean.columns)} 列")
    
    # 計算總庫存價值
    total_value = df_clean['Inventory_Value'].sum()
    print(f"   ✓ 總庫存價值: ${total_value:,.2f}\n")
    
    # ===== 數據質量報告 =====
    print("=== 數據清洗摘要 ===")
    print(f"總記錄數: {len(df_clean)}")
    print(f"\n類別分佈:")
    print(df_clean['Category'].value_counts())
    print(f"\n庫存狀態分佈:")
    print(df_clean['Stock_Status'].value_counts())
    print(f"\n單價統計:")
    print(df_clean['Unit_Cost'].describe())
    print(f"\n庫存統計:")
    print(df_clean['Current_Stock'].describe())
    
    return df_clean


if __name__ == "__main__":
    # 執行清洗
    input_file = "Supply_Chain_Inventory_Dirty_10k.csv"
    output_file = "Supply_Chain_Inventory_Clean.csv"
    
    print("=" * 60)
    print("📊 供應鏈庫存數據清洗 ETL 系統")
    print("=" * 60)
    print()
    
    clean_df = clean_supply_chain_data(input_file, output_file)
    
    print("=" * 60)
    print("✅ 數據清洗完成！所有步驟已成功執行。")
    print("=" * 60)
