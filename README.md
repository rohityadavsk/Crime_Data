# 🚓 Crime Data Processing Pipeline

![Pipeline Overview](https://raw.githubusercontent.com/rohit/crime_data_processor/main/docs/images/pipeline_overview.png)

A robust, multi-environment data engineering pipeline for processing crime data using PySpark and Delta Lake, with seamless local and Databricks integration.

---

## 🌟 Features

- **Multi-environment support:** dev (local), pre-prod, and prod (Databricks)
- **Delta Lake integration:** Reliable, scalable data storage and processing
- **Flexible configuration:** Easily switch environments and data locations
- **Robust logging:** Timestamped logs for every run, stored in `logs/`
- **Automated CI/CD:** GitHub Actions pipeline for testing, artifact management, and Databricks job orchestration
- **Extensive testing:** Pytest-based test suite for all core logic
- **Easy onboarding:** Clear structure, docstrings, and usage examples

---

## 🗂️ Project Structure

```plaintext
.
├── data/
│   ├── input/
│   └── output/
├── logs/
├── src/
│   ├── config.py
│   ├── data_processor.py
│   ├── logger.py
│   ├── main.py
│   └── databricks_scripts/
│       ├── read_data.py
│       ├── process_data.py
│       └── write_data.py
├── tests/
│   ├── test_data_processor.py
│   └── test_helpers.py
├── .github/
│   └── workflows/
│       └── crime_data_pipeline.yml
├── requirements.txt
├── setup.py
└── README.md
```

---

## 🚀 Quickstart

### 1. Clone & Set Up

```bash
git clone https://github.com/yourusername/crime_data_processor.git
cd crime_data_processor
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment

- Copy `.env.example` to `.env` and fill in any required secrets (for Databricks/Azure).
- For Databricks, set secrets in your GitHub repo as described below.

### 3. Run Locally (Dev)

```bash
python src/main.py dev
```

or simply

```bash
python src/main.py
```
_(defaults to dev if no argument or ENVIRONMENT variable is set)_

### 4. Run Tests

```bash
pytest tests/
```

---

## ⚙️ Environments

| Environment | How to Run Locally                | How Run in CI/CD (GitHub Actions)         | Storage/Compute         |
|-------------|-----------------------------------|-------------------------------------------|------------------------|
| dev         | `python src/main.py dev`          | On push to main branch                    | Local filesystem       |
| pre-prod    | `python src/main.py pre-prod`     | On promotion, triggers Databricks job     | Databricks + DBFS      |
| prod        | `python src/main.py prod`         | On promotion, triggers Databricks job     | Databricks + DBFS      |

---

## 🏗️ CI/CD Pipeline

![CI/CD Flow](https://raw.githubusercontent.com/rohit/crime_data_processor/main/docs/images/cicd_flow.png)

- **Dev:** Runs tests and pipeline locally, uploads artifacts.
- **Pre-prod/Prod:** Downloads artifacts, installs dependencies, triggers Databricks jobs using the Databricks CLI.
- **Logs:** Collected and uploaded as artifacts for every environment.

**Workflow file:** `.github/workflows/crime_data_pipeline.yml`

---

## 🔑 Secrets & Databricks Integration

Set these secrets in your GitHub repository for pre-prod/prod Databricks jobs:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_CLUSTER_ID`
- `DATABRICKS_PREPROD_JOB_ID`
- `DATABRICKS_PROD_JOB_ID`

Your Databricks jobs should be configured to run the pipeline and accept the `ENVIRONMENT` parameter.

---

## 📝 Logging

- All logs are stored in the `logs/` directory, with timestamps for each run.
- Logs are also uploaded as artifacts in CI/CD for traceability.

---

## 🧪 Testing

- Run all tests:  
  ```bash
  pytest tests/
  ```
- Test coverage:  
  ```bash
  pytest --cov=src tests/
  ```

---

## 🧑‍💻 Contributing & Onboarding

- Start with this README and the [project structure](#project-structure).
- Review `src/main.py` for the main entry point and environment handling.
- See `src/data_processor.py` for the core pipeline logic.
- Check `src/databricks_scripts/` for Databricks job scripts.
- Review `.github/workflows/crime_data_pipeline.yml` for CI/CD details.
- For questions, check docstrings or ask a teammate!

---

## 📸 Example Screenshots

![Sample Log Output](https://raw.githubusercontent.com/rohit/crime_data_processor/main/docs/images/sample_log.png)
![Databricks Job Run](https://raw.githubusercontent.com/rohit/crime_data_processor/main/docs/images/databricks_job.png)

---

## 📬 Questions?

Open an issue or contact the maintainers!

---

**Happy data engineering! 🚦**

---

> _Tip: Add your own screenshots or diagrams to the `docs/images/` folder and update the image links above for a more personalized README!_ 