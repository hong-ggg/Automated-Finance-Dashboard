import yfinance as yf
import pandas as pd
import datetime
import os
import pyodbc
from selenium import webdriver
from selenium.webdriver.common.by import By

# 定義要抓取的指數及其代碼
indices = {
    "台灣加權指數": "^TWII",
    "NASDAQ": "^IXIC",
    "日經指數": "^N225",
    "恆生指數": "^HSI",
    "上證指數": "000001.SS",
    "布蘭特原油": "BZ=F",
    "天然氣": "NG=F",
    "黃金": "GC=F",
    "小麥": "ZW=F",
    "黃豆": "ZS=F",
    "玉米": "ZC=F"
}

# 定義要抓取的匯率及其代碼
currencies = {
    "USD_TWD": "TWD=X",
    "USD_JPY": "JPY=X",
    "USD_CNY": "CNY=X",
    "EUR_USD": "EURUSD=X",
    "GBP_USD": "GBPUSD=X"
}

# 設定日期範圍
end_date = datetime.datetime.now()
start_date = end_date - datetime.timedelta(days=180)

# 建立存放檔案的資料夾
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# 資料庫連線設定
def connect_to_db():
    """
    建立與資料庫的連線。
    返回資料庫連線物件。
    """
    connection = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=YOUR-SERVER;'
        'DATABASE=MarketData;'
        'Trusted_Connection=yes;'
    )
    return connection

# 判斷欄位型別
def determine_column_type(value):
    """
    根據欄位值判斷資料型別，用於資料表結構定義。
    """
    if isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, str):
        return "NVARCHAR(MAX)"
    elif isinstance(value, bool):
        return "BIT"
    elif isinstance(value, datetime.datetime):
        return "DATETIME"
    else:
        return "NVARCHAR(MAX)"

# 建立資料表並儲存資料
def create_and_save_to_table(table_name, connection, df):
    """
    將資料儲存到資料庫表，必要時自動建立表結構。
    """
    cursor = connection.cursor()

    # 建立資料表結構
    column_definitions = []
    for col in df.columns:
        sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else ""
        if col == "日期":
            column_definitions.append(f"{col} DATE NOT NULL")
        else:
            column_type = determine_column_type(sample_value)
            column_definitions.append(f"{col} {column_type}")

    columns_definition = ", ".join(column_definitions)

    cursor.execute(f'''
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{table_name}'
        )
        BEGIN
            CREATE TABLE {table_name} (
                {columns_definition}
            )
        END
    ''')
    connection.commit()
    print(f"Table '{table_name}' checked/created successfully.")

    # 處理並插入數據
    df = replace_inf_with_zero(df.fillna(0))
    for _, row in df.iterrows():
        placeholders = ", ".join(["?"] * len(row))
        columns = ", ".join(df.columns)
        update_clause = ", ".join([f"{col} = ?" for col in df.columns if col != "日期" and col != "名稱"])

        sql_update = f"""
            UPDATE {table_name}
            SET {update_clause}
            WHERE 日期 = ? AND 名稱 = ?
        """

        sql_insert = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
        """

        update_values = tuple(row[col] for col in df.columns if col != "日期" and col != "名稱")
        condition_values = tuple(row[["日期", "名稱"]])
        insert_values = tuple(row)

        try:
            if cursor.execute(sql_update, update_values + condition_values).rowcount == 0:
                cursor.execute(sql_insert, insert_values)
        except pyodbc.IntegrityError:
            print(f"Error inserting/updating entry: {row}")
    connection.commit()
    print(f"Data successfully saved to table '{table_name}'.")

# 計算技術指標
def calculate_indicators(data):
    """
    計算基本技術指標，如漲跌幅與成交量變化。
    """
    data["前一天收盤價"] = data["收盤價"].shift(1)
    data["Price_Change"] = data["收盤價"] - data["前一天收盤價"]
    data["Percentage_Change"] = (data["Price_Change"] / data["前一天收盤價"] * 100).round(2)
    if "成交量" in data.columns:
        data["成交量"] = data["成交量"].astype("float", errors="ignore")
        data["前一天成交量"] = data["成交量"].shift(1)
        data["Volume_Change"] = data["成交量"] - data["前一天成交量"]
        data["Volume_Percentage_Change"] = (data["Volume_Change"] / data["前一天成交量"] * 100).round(2)
    return data

# 處理指數數據
def process_and_save_indices():
    """
    抓取所有指數數據，儲存至 CSV 並寫入資料庫。
    """
    for name, symbol in indices.items():
        print(f"正在抓取{name} ({symbol}) 的數據...")
        try:
            data = yf.download(symbol, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
            data = data.reset_index()
            data["名稱"] = name
            data = data[["Date", "名稱", "Open", "High", "Low", "Close", "Volume"]]
            data.columns = ["日期", "名稱", "開盤價", "最高價", "最低價", "收盤價", "成交量"]
            data["日期"] = data["日期"].dt.strftime("%Y/%m/%d")

            data = calculate_indicators(data)

            output_file = os.path.join(output_dir, f"{name}.csv")
            data.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"{name} 的數據已儲存到 {output_file}")

            connection = connect_to_db()
            create_and_save_to_table(name, connection, data)
        except Exception as e:
            print(f"抓取{name}的數據時出現錯誤: {e}")

# 處理匯率數據
def process_and_save_currencies():
    """
    抓取所有匯率數據，儲存至 CSV 並寫入資料庫。
    """
    for name, symbol in currencies.items():
        print(f"正在抓取{name} ({symbol}) 的數據...")
        try:
            data = yf.download(symbol, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
            data = data.reset_index()
            data["名稱"] = name
            data = data[["Date", "名稱", "Open", "High", "Low", "Close"]]
            data.columns = ["日期", "名稱", "開盤價", "最高價", "最低價", "收盤價"]
            data["日期"] = data["日期"].dt.strftime("%Y/%m/%d")

            data = calculate_indicators(data)

            output_file = os.path.join(output_dir, f"{name}.csv")
            data.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"{name} 的數據已儲存到 {output_file}")

            connection = connect_to_db()
            create_and_save_to_table(name, connection, data)
        except Exception as e:
            print(f"抓取{name}的數據時出現錯誤: {e}")

# 處理台灣經濟指標數據
def process_and_save_indicators(driver):
    """
    使用 Selenium 抓取台灣經濟指標數據，儲存至 CSV 並寫入資料庫。
    """
    print("Fetching economic calendar data...")
    driver.get("https://tradingeconomics.com/taiwan/indicators")

    columns = ['日期', '名稱', '現值', '前值', '最高', '最低']
    calendar_data = pd.DataFrame(columns=columns)

    for i in range(4, 8):
        try:
            row_data = {}
            try:
                row_data["日期"] = driver.find_element(
                    By.XPATH, f'//div[@class = "table-responsive"]//tbody/tr[{i}]/td[7]'
                ).text.strip()
            except Exception:
                row_data["日期"] = "N/A"

            try:
                row_data["名稱"] = driver.find_element(
                    By.XPATH, f'//div[@class = "table-responsive"]//tbody/tr[{i}]/td[1]'
                ).text.strip()
            except Exception:
                row_data["現值"] = "N/A"

            try:
                row_data["現值"] = driver.find_element(
                    By.XPATH, f'//div[@class = "table-responsive"]//tbody/tr[{i}]/td[2]'
                ).text
            except Exception:
                row_data["現值"] = "N/A"

            try:
                row_data["前值"] = driver.find_element(
                    By.XPATH, f'//div[@class = "table-responsive"]//tbody/tr[{i}]/td[3]'
                ).text
            except Exception:
                row_data["前值"] = "N/A"

            try:
                row_data["最高"] = driver.find_element(
                    By.XPATH, f'//div[@class = "table-responsive"]//tbody/tr[{i}]/td[4]'
                ).text
            except Exception:
                row_data["最高"] = "N/A"

            try:
                row_data["最低"] = driver.find_element(
                    By.XPATH, f'//div[@class = "table-responsive"]//tbody/tr[{i}]/td[5]'
                ).text
            except Exception:
                row_data["最低"] = "N/A"

            calendar_data = pd.concat([calendar_data, pd.DataFrame([row_data])], ignore_index=True)
            calendar_data.iloc[:, 2:] = calendar_data.iloc[:, 2:].apply(pd.to_numeric, errors='coerce')
        except Exception as e:
            print(f"Failed to fetch calendar data for row {i}: {e}")

    output_file = os.path.join(output_dir, f"taiwan_indicators.csv")
    calendar_data.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"taiwan_indicators 的數據已儲存到 {output_file}")

    calendar_data['日期'] = translate_date_series(calendar_data['日期'])
    connection = connect_to_db()
    create_and_save_to_table("Taiwan_Economic_Data", connection, calendar_data)
    print("Taiwan economic calendar data saved successfully.")

# 日期轉換函數
def translate_date_series(date_series):
    """
    將 "Jan/23" 格式的日期轉換為 "2023-01-01" 格式。
    """
    def translate_date(date_str):
        return datetime.datetime.strptime(date_str, '%b/%y').strftime('%Y-%m-%d')

    return date_series.apply(translate_date)

# 替換無限值
def replace_inf_with_zero(df):
    """
    將 DataFrame 中的無限值替換為 0。
    """
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = df[column].replace([float('inf'), float('-inf')], 0)
    return df

if __name__ == "__main__":
    # 處理指數和匯率數據
    process_and_save_indices()
    process_and_save_currencies()

    # 使用 Selenium 抓取經濟指標數據
    driver = webdriver.Chrome()
    process_and_save_indicators(driver)
    driver.quit()