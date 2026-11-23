# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 06 · DevOps: Git folders, bundles, tests and CI/CD
# MAGIC
# MAGIC Course: **DevOps Essentials for Data Engineering**
# MAGIC
# MAGIC Everything you built is already code in this repo:
# MAGIC
# MAGIC ```
# MAGIC pedalhouse/
# MAGIC ├── databricks.yml              the bundle: name, variables, dev and prod targets
# MAGIC ├── resources/                  WHAT to deploy (pipeline, jobs) as YAML
# MAGIC ├── src/pedalhouse/             production Python code (tested)
# MAGIC ├── src/pipeline/               Declarative Pipeline source (SQL + Python)
# MAGIC ├── src/jobs/                   job task notebooks
# MAGIC ├── labs/                       these notebooks (not deployed as jobs)
# MAGIC ├── tests/                      unit and integration tests (pytest)
# MAGIC └── .github/workflows/ci.yml    CI: tests + bundle validate, deploy on demand
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Git folders
# MAGIC
# MAGIC 1. Push this project to your own GitHub repository.
# MAGIC 2. In Databricks: **Workspace > Create > Git folder**, paste the repo URL. (For a private
# MAGIC    repo, link GitHub first in **Settings > Linked accounts**, with the GitHub app or a
# MAGIC    personal access token.)
# MAGIC 3. In the Git folder, click the branch name: create a branch `feature/hourly-demand`.
# MAGIC 4. Make a change (next exercise), then **Commit & push** from the same dialog and open a
# MAGIC    pull request on GitHub.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run the unit tests here, on Databricks
# MAGIC
# MAGIC `tests/test_transforms.py` tests the business logic on tiny DataFrames built with
# MAGIC `spark.createDataFrame`, compared with `assertDataFrameEqual`. No tables, no files:
# MAGIC fast and deterministic. The same tests run on your laptop and in GitHub Actions.

# COMMAND ----------

# MAGIC %pip install pytest --quiet

# COMMAND ----------

import os
import sys

import pytest

sys.path.append(os.path.abspath("../src"))
exit_code = pytest.main(["-q", "-p", "no:cacheprovider", os.path.abspath("../tests/test_transforms.py")])
print("Tests passed" if exit_code == 0 else f"Tests failed (exit code {exit_code})")

# COMMAND ----------

# MAGIC %md
# MAGIC **Prove the test can fail.** Open `src/pedalhouse/transforms.py`, change
# MAGIC `UNLOCK_FEE = 1.00` to `1.50`, and run the tests again: `test_base_fare` fails and shows
# MAGIC exactly which rows differ. Put it back.
# MAGIC
# MAGIC ## 3. The bundle, line by line
# MAGIC
# MAGIC Open `databricks.yml` and `resources/*.yml` next to this notebook.
# MAGIC
# MAGIC | Concept | In this repo |
# MAGIC |---|---|
# MAGIC | variables | `catalog`, `schema`: the only things that change per environment |
# MAGIC | targets | `dev` (development mode: `[dev you]` prefix, paused schedules) and `prod` |
# MAGIC | resource references | the job's `pipeline_task` uses `${resources.pipelines.pedalhouse_pipeline.id}` |
# MAGIC | workspace paths | the pipeline gets `src_path: ${workspace.file_path}/src` |
# MAGIC | include | `resources/*.yml` keeps one resource per file |
# MAGIC
# MAGIC ### Deploy from the workspace UI
# MAGIC Open `databricks.yml` > **Deployments** panel > target `dev` > **Deploy**.
# MAGIC
# MAGIC ### Deploy from a terminal (Databricks CLI)
# MAGIC ```bash
# MAGIC databricks auth login --host https://<your-workspace>.cloud.databricks.com
# MAGIC databricks bundle validate                     # catches YAML and reference errors
# MAGIC databricks bundle deploy -t dev                # creates/updates the dev resources
# MAGIC databricks bundle run pedalhouse_daily -t dev  # runs the job
# MAGIC databricks bundle summary -t dev               # links to everything deployed
# MAGIC databricks bundle deploy -t prod               # same code, prod schema
# MAGIC databricks bundle destroy -t dev               # removes the dev resources
# MAGIC ```
# MAGIC To use your own catalog: `databricks bundle deploy -t dev --var catalog=learning`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dev and prod are isolated
# MAGIC
# MAGIC Deploy both targets, run `pedalhouse-daily` in each, then run the next cell.
# MAGIC Same code, two schemas, two independent sets of tables.

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_schema, COUNT(*) AS tables
    FROM {paths.catalog}.information_schema.tables
    WHERE table_schema IN ('pedalhouse_dev', 'pedalhouse')
    GROUP BY table_schema
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. CI/CD with GitHub Actions
# MAGIC
# MAGIC `.github/workflows/ci.yml` runs on every pull request and push to `main`:
# MAGIC
# MAGIC 1. **unit-tests**: installs Java, PySpark and Delta, runs `pytest` (no Databricks needed).
# MAGIC 2. **bundle-validate**: runs `databricks bundle validate` if the repository has the
# MAGIC    secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.
# MAGIC 3. **deploy-prod**: manual (**Run workflow** button), deploys the `prod` target.
# MAGIC
# MAGIC Add the secrets in GitHub: **Settings > Secrets and variables > Actions**. For a real
# MAGIC team, use a **service principal** with OAuth instead of a personal token.
# MAGIC
# MAGIC ## 6. The full loop (final exercise)
# MAGIC
# MAGIC 1. On branch `feature/hourly-demand`, add `src/pipeline/gold/05_gold_hourly_demand.sql`
# MAGIC    (Lab 03 challenge).
# MAGIC 2. Add a unit test for any function you touch.
# MAGIC 3. Commit, push, open a pull request: watch CI run the tests.
# MAGIC 4. Deploy `dev` from the Deployments panel, run the job, check the new table.
# MAGIC 5. Merge, then run the **deploy-prod** workflow.
# MAGIC
# MAGIC That is the whole DevOps course in one loop: **branch, test, review, deploy dev, promote to prod.**
