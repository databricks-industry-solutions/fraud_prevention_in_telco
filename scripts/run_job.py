# Upgrade Databricks SDK and restart Python to see updated packages
# %pip install --upgrade databricks-sdk
# %restart_python

"""
Reset or create the Fraud Data Pipeline job from the workspace.
After cloning the repo (Blueprint style), repo root is typically at
/Workspace/Repos/<user>/<repo>/ and pipeline scripts live under notebooks/.
Set REPO_ROOT to your workspace repo path and JOB_ID to update an existing job.
"""
from databricks.sdk.service.jobs import JobSettings as Job

# After bundle deploy or repo clone, set to your repo root (e.g. /Workspace/Repos/user/datapipeline/)
REPO_ROOT = "/Workspace/Repos/your-user/your-repo"
JOB_ID = None  # Set to existing job ID to reset, or None to create new

fraud_data_pipeline = Job.from_dict(
    {
        "name": "fraud_data_pipeline",
        "tasks": [
            {"task_key": "Device_ID_Reference", "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_device_id_reference.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Cell_Registry", "depends_on": [{"task_key": "Device_ID_Reference"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_cell_registry.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Raw_Network_Data", "depends_on": [{"task_key": "Cell_Registry"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_raw_network_data.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Bronze_Network_Data", "depends_on": [{"task_key": "Raw_Network_Data"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_bronze_network_data.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Silver_Network_Data", "depends_on": [{"task_key": "Bronze_Network_Data"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_silver_network_data.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Gold_Network_Data", "depends_on": [{"task_key": "Silver_Network_Data"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_gold_network_data.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Bronze_Device_SDK", "depends_on": [{"task_key": "Device_ID_Reference"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_bronze_device_sdk.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Bronze_Transactions", "depends_on": [{"task_key": "Device_ID_Reference"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_bronze_app_transactions.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Silver_Device_SDK", "depends_on": [{"task_key": "Bronze_Device_SDK"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_silver_device_sdk.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Gold_Device_SDK", "depends_on": [{"task_key": "Silver_Device_SDK"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_gold_device_sdk.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Silver_Transactions", "depends_on": [{"task_key": "Bronze_Transactions"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_silver_app_transactions.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Gold_Transactions", "depends_on": [{"task_key": "Silver_Transactions"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/generate_gold_app_transactions.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "Risk_Engine", "depends_on": [{"task_key": "Gold_Transactions"}, {"task_key": "Gold_Device_SDK"}, {"task_key": "Gold_Network_Data"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/risk_engine.py"}, "environment_key": "fraud_data_pipeline_environment"},
            {"task_key": "analyst_simulation", "depends_on": [{"task_key": "Risk_Engine"}], "spark_python_task": {"python_file": f"{REPO_ROOT}/notebooks/run_analyst_simulation.py"}, "environment_key": "fraud_data_pipeline_environment"},
        ],
        "queue": {"enabled": True},
        "environments": [
            {"environment_key": "Default", "spec": {"client": "1"}},
            {"environment_key": "fraud_data_pipeline_environment", "spec": {"dependencies": ["pandas>=1.5.0", "numpy>=1.21.0", "scikit-learn>=1.1.0", "matplotlib>=3.5.0", "seaborn>=0.11.0", "hdbscan>=0.8.0", "faker>=18.0.0"], "client": "1"}},
        ],
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
if JOB_ID:
    w.jobs.reset(new_settings=fraud_data_pipeline, job_id=JOB_ID)
else:
    w.jobs.create(**fraud_data_pipeline.as_dict())
