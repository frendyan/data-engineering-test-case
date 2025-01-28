# ETL Project with PostgreSQL

A simple ETL project that extract data from API, transform and model into warehouse tables then visualize it on metabase

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- pip (Python package manager)

## Technologies Used

- Apache Airflow: Data Orchestration
- PostgreSQL: Data persistence
- SQLAlchemy: SQL toolkit and ORM
- Docker: Containerization
- Python: Programming language
- Metabase: Visualization Tools

## Installation

1. Clone the repository and locate to project folder:
```bash
cd etl-project-api
```

2. Excute `run_docker.sh`
```bash
./run_docker.sh
```

3. Connect to postgres and create 2 schema `staging` and `warehouse` in db `dwh`

4. Open this URL on local browser
```bash
http://localhost:8080   #airflow, use admin:admin for login
http://localhost:3000   #metabase
```
