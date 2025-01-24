# Stock Market Dashboard

This repository contains a Python-based project to automate financial and economic data retrieval, storage, and visualization. It integrates data scraping with `yfinance` and `selenium`, a local database for storage, and Tableau for dynamic dashboards. Below is an overview of the project structure and usage.

- Access the output dashboard using the link below:
https://public.tableau.com/app/profile/.54857028/viz/OverviewDashboard_17376245407830/2?publish=yes

## Project Structure

```
Stock Market Dashboard/
├── data/                # Folder for backup CSV files
│   ├── EUR_USD.csv
│   ├── GBP_USD.csv
│   ├── NASDAQ.csv
│   ├── taiwan_indicators.csv
│   ├── USD_CNY.csv
│   ├── USD_JPY.csv
│   ├── USD_TWD.csv
│   ├── ... (other financial and economic indicator files)
├── data_source.py                 # Python script to scrape and process data
├── Overview Dashboard.twbx        # Tableau workbook for the dashboard
└── README.md                      # Project documentation (this file)
```

## Prerequisites

1. **Python Libraries**:
   - `yfinance`: For fetching financial data.
   - `selenium`: For web scraping macroeconomic data.
   - Database libraries (e.g., `sqlite3` or `sqlalchemy`) for data storage.

2. **Tableau**:
   - Install Tableau Desktop to connect to the local database and visualize data.

3. **Browser Driver**:
   - Install the correct browser driver (e.g., ChromeDriver) for Selenium.

## Setup and Usage

### 1. Clone the Repository
```bash
git clone https://github.com/your_username/stock-market-dashboard.git
cd stock-market-dashboard
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure the Database
- Set up a local database (e.g., SQLite) to store the scraped data.
- Update the database connection details in `data_source.py`.

### 4. Run the Python Script
```bash
python data_source.py
```
This script will:
- Fetch financial data using `yfinance`.
- Scrape macroeconomic indicators using `selenium`.
- Store the data in the local database.

### 5. Update Tableau Dashboard
- Open `Overview Dashboard.twbx` in Tableau.
- Ensure the data source points to the updated local database.
- Refresh the data to view the latest updates.

## Additional Notes

- **Data Backup**: The `data` folder contains CSV backups of previously fetched data.
- **Automation**: Schedule the Python script to run daily using a task scheduler (e.g., cron on Linux or Task Scheduler on Windows).

## License
This project is licensed under the MIT License. See the LICENSE file for details.

## Contributing
Contributions are welcome! Please fork the repository and create a pull request for any changes.

---

For any questions or issues, please feel free to open an issue or contact me directly.