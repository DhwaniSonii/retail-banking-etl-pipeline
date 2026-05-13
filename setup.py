from setuptools import setup, find_packages

setup(
    name="banking-data-platform",
    version="1.0.0",
    description="Production-grade retail banking ETL pipeline with data quality, governance, and lineage",
    author="Dhwani Soni",
    python_requires=">=3.10",
    packages=find_packages(exclude=["tests*", "notebooks*", "dashboard*"]),
    install_requires=[
        "pandas>=2.1.0",
        "numpy>=1.26.0",
        "pyarrow>=14.0.0",
        "sqlalchemy>=2.0.0",
        "psycopg2-binary>=2.9.0",
        "faker>=20.0.0",
        "python-dotenv>=1.0.0",
        "pyyaml>=6.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "flake8>=6.1.0",
            "bandit>=1.7.0",
        ],
        "orchestration": [
            "apache-airflow>=2.7.0",
        ],
        "dbt": [
            "dbt-core>=1.6.0",
            "dbt-postgres>=1.6.0",
            "dbt-utils>=1.1.0",
        ],
        "dashboard": [
            "streamlit>=1.28.0",
            "plotly>=5.18.0",
        ],
        "quality": [
            "great-expectations>=0.18.0",
        ],
        "notebooks": [
            "jupyter>=1.0.0",
            "matplotlib>=3.8.0",
            "seaborn>=0.13.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "banking-pipeline=etl.run_pipeline:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Office/Business :: Financial",
    ],
)
