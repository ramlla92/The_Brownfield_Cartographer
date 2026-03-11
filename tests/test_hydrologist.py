import pytest
from pathlib import Path
from src.analyzers.python_dataflow import PythonDataFlowAnalyzer

def test_python_dataflow_basic(tmp_path):
    # Setup mock python file
    code = """
import pandas as pd
df = pd.read_csv("data/raw.csv")
df.to_sql("processed_table", con=engine)
"""
    file_path = tmp_path / "app.py"
    file_path.write_text(code)

    analyzer = PythonDataFlowAnalyzer()
    results = analyzer.analyze(file_path)

    assert len(results) == 1
    node = results[0]
    
    # Check read_csv
    assert "data/raw.csv" in node.source_datasets
    
    # Check to_sql
    assert "processed_table" in node.target_datasets
    assert node.transformation_type == "python_dataflow"

def test_python_dataflow_dynamic(tmp_path):
    code = """
import pandas as pd
filename = f"data_{date}.csv"
df = pd.read_csv(filename)
"""
    file_path = tmp_path / "dynamic_app.py"
    file_path.write_text(code)

    analyzer = PythonDataFlowAnalyzer()
    results = analyzer.analyze(file_path)

    assert len(results) == 1
    assert results[0].source_datasets == ["dynamic_reference"]

def test_sql_lineage_basic(tmp_path):
    code = """
CREATE TABLE analytics.revenue_daily AS
SELECT
    user_id,
    SUM(amount) as daily_revenue
FROM raw.payments
JOIN raw.users ON payments.user_id = users.id
WHERE status = 'success'
GROUP BY 1
"""
    file_path = tmp_path / "revenue.sql"
    file_path.write_text(code)

    from src.analyzers.sql_lineage import SQLLineageAnalyzer
    analyzer = SQLLineageAnalyzer()
    result = analyzer.analyze(file_path)

    assert result is not None
    assert "raw.payments" in result.source_datasets
    assert "raw.users" in result.source_datasets
    assert "analytics.revenue_daily" in result.target_datasets

def test_sql_lineage_dbt(tmp_path):
    code = """
SELECT *
FROM {{ ref('stg_payments') }}
JOIN {{ source('raw', 'users') }} ON 1=1
"""
    file_path = tmp_path / "dbt_model.sql"
    file_path.write_text(code)

    from src.analyzers.sql_lineage import SQLLineageAnalyzer
    analyzer = SQLLineageAnalyzer()
    result = analyzer.analyze(file_path)

    assert result is not None
    assert "stg_payments" in result.source_datasets
    assert "raw.users" in result.source_datasets

def test_sql_lineage_cte(tmp_path):
    code = """
WITH filtered_payments AS (
    SELECT * FROM raw.payments WHERE amount > 100
)
INSERT INTO analytics.large_payments
SELECT * FROM filtered_payments
"""
    file_path = tmp_path / "insert.sql"
    file_path.write_text(code)

    from src.analyzers.sql_lineage import SQLLineageAnalyzer
    analyzer = SQLLineageAnalyzer()
    result = analyzer.analyze(file_path)

    assert result is not None
    assert "raw.payments" in result.source_datasets
    assert "filtered_payments" not in result.source_datasets
    assert "analytics.large_payments" in result.target_datasets

def test_dag_config_airflow(tmp_path):
    code = """
from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG('test_dag') as dag:
    start = DummyOperator(task_id='start')
    process = DummyOperator(task_id='process')
    end = DummyOperator(task_id='end')
    
    start >> process >> end
"""
    file_path = tmp_path / "my_dag.py"
    file_path.write_text(code)

    from src.analyzers.dag_config_parser import DAGConfigAnalyzer
    analyzer = DAGConfigAnalyzer()
    results = analyzer.analyze(file_path)

    assert len(results) == 2
    assert results[0].source_datasets == ["start"]
    assert results[0].target_datasets == ["process"]
    assert results[1].source_datasets == ["process"]
    assert results[1].target_datasets == ["end"]

def test_dag_config_dbt_schema(tmp_path):
    code = """
version: 2
models:
  - name: my_model
    description: "A test model"
sources:
  - name: raw
    tables:
      - name: users
"""
    file_path = tmp_path / "schema.yml"
    file_path.write_text(code)

    from src.analyzers.dag_config_parser import DAGConfigAnalyzer
    analyzer = DAGConfigAnalyzer()
    results = analyzer.analyze(file_path)

    # 1 model + 1 source table
    assert len(results) == 2
    model_node = next(r for r in results if r.transformation_type == "dbt")
    source_node = next(r for r in results if r.transformation_type == "dbt_source")
    
    assert model_node.target_datasets == ["my_model"]
    assert source_node.target_datasets == ["raw.users"]

def test_hydrologist_full_run(tmp_path):
    # Setup a mini-repo
    (tmp_path / "sql").mkdir()
    (tmp_path / "app").mkdir()
    
    # 1. SQL model
    (tmp_path / "sql" / "revenue.sql").write_text("""
CREATE TABLE analytics.revenue AS SELECT * FROM raw.payments
""")
    # 2. Python app
    (tmp_path / "app" / "export.py").write_text("""
import pandas as pd
df = pd.read_sql("SELECT * FROM analytics.revenue", con=None)
df.to_csv("s3://bucket/export.csv")
""")
    # 3. Airflow DAG
    (tmp_path / "app" / "my_dag.py").write_text("""
extract >> transform >> load
""")
    
    from src.agents.hydrologist import Hydrologist
    hydro = Hydrologist(repo_root=tmp_path)
    graph = hydro.run()
    
    assert len(graph.transformations) >= 4
    
    assert "raw.payments" in graph.datasets
    assert "analytics.revenue" in graph.datasets
    assert "s3://bucket/export.csv" in graph.datasets
    
    # Verify sources and sinks
    assert "raw.payments" in graph.source_datasets
    assert "s3://bucket/export.csv" in graph.sink_datasets
    
    # Verify blast radius
    downstream = hydro.blast_radius("raw.payments")
    assert "analytics.revenue" in downstream
    assert "s3://bucket/export.csv" in downstream
