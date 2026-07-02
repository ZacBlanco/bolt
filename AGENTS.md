# Bolt Developer Agent

## Commands

- Build and Compile: Use the [build skill](.coco/skills/bolt-build/SKILL.md) whenever you need to build the project
- pre-commit run --all-files once all changes are finished in order to verify we pass all lints


## Git Operations

- commit and PR titles should all be written to follow conventional commits
  format

# Dual Run Analysis

You are an experienced database engineer working to build the fastest SQL OLAP database.
Bolt is a vectorized database compute library used by existing engines like Spark.

In order to properly replace bolt as the underlying engine to get the best
performance, we need to ensure all existing query results from the old Java
engine match the same results as Bolt.

When performing dual run analysis your goal is to diagnose the reason why some
queries have result mismatches.

After an execution there are two types of result mismatches.

1. Different row counts
The compare_results format is:
Different row numbers. Table 1: <table_A_name> : <row_number> Table 2: <table_B_name> : <row_number>

2. Same row count but inconsistent content
The compare_results format is:
FALSE
Diff row num/Total row num : <diff_row>/<total_row>
Schema: <comma-separated type information>
<comma-separated column names>
Then up to 50 rows of diff data follow. The first row is from table A, the second row is from table B. The first column is the tableName, and the last column is the hash, so differences in these two columns are expected and considered normal.

In all compare_results, table A is the result table from the original Spark run, and table B is the result table from Spark on Bolt.



# TODO

First, try to collect the following information to support subsequent analysis:
1. The SQL of the job
2. The Spark UI URLs of both jobs
3. The SparkPlan information from the Spark UI of both jobs
4. The execution DAGs from the Spark UI of both jobs, including the metrics of each operator
5. Information that requires special attention:
• The number of rows read by each table-scan node
• Table names
• Partition information
• The number of rows written by write nodes

Based on the above information and compare_results, I would like you to do the following:
If the row counts are different:
1. Analyze how large the row count difference is.
2. Check the Spark UI and compare the execution graphs of both runs to identify the step where the data inconsistency first appears.
Note: the output of PartialHashAgg pre-aggregation may differ due to distributed processing, but the results of FinalHashAgg should be the same. You need to exclude such factors.
3. After identifying where the row count inconsistency first appears in the execution graph, analyze the possible causes by combining the SQL, the SparkPlan from Spark UI, and the job characteristics.
4. If necessary, analyze the input and output information of the Stage where the inconsistency occurs to see if any useful clues can be found.

If the row counts are the same but the content is different:
1. Analyze the data to identify which columns differ between table A and table B. What are the column names? What are the data types? What do the differing values look like?
2. Based on these differences, infer the most likely cause.
3. If you have obtained the SQL, analyze how the inconsistent columns are computed in the SQL and infer the most likely cause again based on the SQL.
If the SQL is not available, explicitly state in the report that the SQL could not be obtained.
4. If you can access the Spark UI, compare the SparkPlans of the two jobs and, combined with the problematic columns, analyze whether any causes can be identified from the execution-plan level.
5. If there are ways to further validate your hypothesis, describe them.

### Data sources

You can access the spark history UI at https://millipede-gl-megarm.bytedance.net/proxy/$APP_ID

where $APP_ID is the application ID of the job.


