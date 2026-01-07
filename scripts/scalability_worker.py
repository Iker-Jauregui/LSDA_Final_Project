# scalability_worker.py (Updated)
import sys
import time
from pyspark.sql import SparkSession
from multiprocessing.pool import ThreadPool
from pyspark import keyword_only
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
import time
import sys
import os

"""
    UK Police Stop and Search Scalability Tool
    
    Param 1: Number of Cores/Partitions
    Param 2: Percentage of data to use (1-100)
    Param 3: Filename to save the results (CSV)
"""

def main(argv):
    # 1. Parse Arguments
    if len(argv) < 3:
        print("Usage: python scalability_worker.py <cores> <percentage> <results_file>")
        sys.exit(1)

    cores = int(argv[0])
    percentage = int(argv[1]) / 100.0  # Convert 10 to 0.1
    filename = argv[2]

    # 2. Initialize Spark Session
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as sql_f

    spark = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(f"UK_Police_Scalability_{cores}cores_{int(percentage*100)}perc")
        .config("spark.driver.memory", "16g")
        .config("spark.executor.memory", "16g")
        .getOrCreate()
    )

    # 3. Load Dataset
    # Assuming the parquet file is in the root directory
    path = "data/processed_data.parquet"
    if not os.path.exists(path):
        print(f"Error: {path} not found.")
        spark.stop()
        sys.exit(1)

    df = spark.read.parquet(path)
    
    # Sample and Repartition as per Slide 14
    df = df.sample(False, percentage, seed=42).repartition(cores).cache()
 
    # Trigger lazy operations before timing
    df.count()

    # 4. Start Measuring Training Phase
    start = time.time()
  
    from pyspark.ml.feature import VectorAssembler, StringIndexer
    from pyspark.ml.classification import DecisionTreeClassifier
    from pyspark.ml import Pipeline

    # Define our specific columns
    categorical_cols = ["age", "gender", "law", "ethnicity", "search_type", "reason", "police_force"]
    numeric_cols = ["is_person", "strip_search", "hour", "month", "dayofweek"]

    # Preprocessing: StringIndexers for categories
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") 
        for c in categorical_cols
    ]

    # Assemble all features
    feature_indices = [f"{c}_idx" for c in categorical_cols] + numeric_cols
    assembler = VectorAssembler(inputCols=feature_indices, outputCol="features", handleInvalid="keep")
    
    # Decision Tree
    # Note: maxBins=64 handles the 42 police forces in your data
    dt = DecisionTreeClassifier(
        labelCol="legal_action_taken", 
        featuresCol="features", 
        maxBins=64
    ) 

    # Building the Pipeline (Preprocessing + Training included)
    pipeline = Pipeline(stages=indexers + [assembler, dt])

    # Fitting the model
    pipeline_model = pipeline.fit(df)

    end = time.time()
    runtime = end - start

    # 5. Output Results
    print(f"Building phase took: {runtime} seconds")    
    
    # Write to the CSV file provided in arguments
    file_exists = os.path.isfile(filename)
    with open(filename, "a") as f:
        # If file is new, add header
        if not file_exists:
            f.write("cores,percentage,runtime\n")
        f.write(f"{cores},{int(percentage*100)},{runtime}\n")
            
    spark.stop()
    
if __name__ == "__main__":
    main(sys.argv[1:])