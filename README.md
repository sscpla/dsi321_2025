# EGAT Real-time Power Generation Scraper

## Overview 

This Python-based utility retrieves live power generation metrics from the Electricity Generating Authority of Thailand (EGAT), accessible via the URL https://www.sothailand.com/sysgen/egat/. Leveraging Selenium, the tool emulates browser behavior to extract live updates from the browser's developer console, where data is asynchronously loaded and refreshed by the website.
The scraper operates in a continuous loop, periodically collecting and recording up-to-date electricity output figures at user-defined time intervals.

## Benefits

- **Live Data Access**:Continuously tracks up-to-the-minute electricity generation information sourced directly from EGAT, offering real-time insight into the national grid's performance.
- **Hands-free Logging**:Automates the retrieval process with scheduled scraping intervals, removing the need for manual entry and supporting long-term data consistency.
- **Forecast-Driven Insights**: Supports machine learning and statistical models aimed at anticipating future power consumption trends, enhancing decision-making and load management.
- **User-Friendly Dashboard**:Converts detailed operational data into visually engaging charts and summaries, allowing both analysts and stakeholders to easily interpret performance metrics.
- **Advanced Scraping Technique**:Utilizes Selenium to pull dynamic updates from browser console messages, showcasing a sophisticated solution for extracting data from highly interactive, JavaScript-driven sites.
- **Time-Series Intelligence**:Compiles rich historical datasets ideal for conducting trend evaluations, detecting irregularities, and supporting retrospective energy analytics.

## Dataset Quality

| Quality Check | Description | Status |
|--------------|-------------|--------|
| Contains at least 1,000 records | Ensures dataset has sufficient volume | ✅ Passing |
| Covers a full 24-hour time range | Verifies complete daily coverage | ✅ Passing |
| At least 90% data completeness | Checks for minimal missing values | ✅ Passing |
| No columns with data type 'object' | Ensures proper data typing | ✅ Passing |
| No duplicate records | Confirms data uniqueness | ✅ Passing |

- Dataset (`parquet\egat_realtime_power_history.parquet`)


## Project structure

```
DSI321_2025/
├── .venv/
│   └──Include
|   └──Lib \site-packages
|    └──pip
|    └──pip-25.0.1.dist-info
|   └──Scripts
|    └──activate
|    └──activate.bat
|    └──activate.fish
|    └──Activate.ps1
|    └──deactivate.bat
|    └──pip.exe
|    └──pip3.13.exe
|    └──python.exe
|    └──pythonw.exe
|   └──.gitignore
|   └──pyvenv.cfg
├── parquet/
│   └── egat_realtime_power_history.parquet
├── test-scraping/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── run_scraper_and_save_to_lakefs.ipynb
├── UI/
│   └── streamlit_app.py
├── .gitignore
├── docker-compose.yml
├── egat_pipeline.py
├── prefect.yaml
├── quality_check.py
└── README.md
```

## Resources

- **Tools Used**
    - **Web Scraping**: Python `webdriver_manager` `Selenium`
    - **Data Validation**: `Pydantic`
    - **Data Storage**: `lakeFS`
    - **Orchestration**: `Prefect`
    - **Visualization**: `Streamlit`
    - **CI/CD**: GitHub Actions

- **Hardware Requirements**
    - Docker-compatible environment
    - Local or cloud system with:
        - At least 4 GB RAM
        - Internet access for EGAT web
        - Port availability for Prefect UI (default: `localhost:4200`)

## Prepare

- **Setup Steps**
    - Create a virtual environment
    ```bash
    python -m venv .venv
    ```
    - Install required packages
    ```bash
    #Windows config
    pip install -r test_scraping\requirements.txt
    ```
    ```bash
    #mac-os
    pip install -r test_scraping\requirements.txt
    ```
    - Activate the virtual environment
    ```bash
    #Windows config
    source .venv/Scripts/activate
    ```
    ```bash
    #mac-os
    source .venv/bin/activate
    ```

## Running Prefect

- Create a deployment for build, push, and pull for building Docker images, pushing code to the Docker registry, and pulling code to run the flow.
- **If there is a PREFECT_API_URL bug, run this script first.**
    ```bash
    #Windows config
    $env:PREFECT_API_URL = "http://127.0.0.1:4200/api"
    ```
    ```bash
    #mac-os
    export PREFECT_API_URL="http://127.0.0.1:4200/api"
    ```
- Deployments allow you to run flows on a schedule and trigger runs based on events.
    ```bash
    prefect deploy
    ```
- Start Prefect Worker to start pulling jobs from the Work Pool and Work Queue.
    ```bash
    pefect worker start --pool 'default-agent-pool' --work-queue 'default'
    ```
- Run Streamlit UI
    ```bash
    streamlit run UI/streamlit_app.py
    ```
