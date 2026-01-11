# 1) Data Collection Script

## Overview

This script `scripts/data_collection.py` was used to collect stop-and-search data from the UK Police API and download it into the `repositorio/Grupo1` directory.

## How It Works

1. **Fetch Available Dates**: The script queries the UK Police API to get all available months with data. 

2. **Iterate Through Dates and Forces**: For each date, it retrieves the list of police forces that have stop-and-search data available.

3. **Collect Records**: For each force and date combination, the script: 
   - Makes an API request to fetch stop-and-search records
   - Adds date and force_id metadata to each record
   - Aggregates all records in memory

4. **Rate Limiting**:  Implements a 0.3-second delay between requests to avoid overwhelming the API.

5. **Data Storage**: The collected data is: 
   - Normalized using `pandas. json_normalize()` to flatten nested JSON structures
   - Saved as a Parquet file (`stop_search_data.parquet`) for efficient storage and analysis

## Output

- **Format**:  Parquet file
- **Location**: `repositorio/Grupo1`
- **Content**: Complete stop-and-search records from UK Police API with date and force identifiers

## API Source

Data source: [UK Police Data API](https://data.police.uk/api)

# 2) Scalability Worker Script

## Overview

The `scripts/scalability_worker.py` script is a utility for conducting scalability studies on the UK Police Stop and Search dataset using PySpark and Machine Learning pipelines. This script is used by the `uk_police_notebook.ipynb` notebook at section **"5. Scalabilty study: Size up, Scale up and Speed up"**.

## Purpose

This tool measures the performance of a Decision Tree classifier under different computational configurations to analyze: 
- **Size-up**: How performance scales with increasing data size (fixed cores)
- **Speed-up**: How performance improves with more cores (fixed data size)
- **Scale-up**: How performance scales when both data size and cores increase proportionally

## Parameters

The script accepts 5 command-line arguments:

1. **Study Type**: Type of scalability study (`'size-up'`, `'speed-up'`, or `'scale-up'`)
2. **Times**: Number of times to repeat the study for statistical reliability
3. **Cores**: Number of CPU cores/partitions to use
4. **Percentage**: Percentage of data to use (1-100)
5. **Results File**: CSV filename to save the results

## How It Works

1. **Initialization**: Creates a Spark session with the specified number of cores
2. **Data Loading**: Loads the processed parquet data from `data/processed_data.parquet`
3. **Data Sampling**: Samples the specified percentage of data and repartitions it across cores
4. **Warm-up**:  Performs 3 warm-up runs (not logged) to stabilize JVM and Spark
5. **Measurement**: Runs the full ML pipeline (preprocessing + training) multiple times: 
   - String indexing for categorical features (age, gender, law, ethnicity, etc.)
   - Feature assembly (categorical indices + numeric features)
   - Decision Tree classifier training
6. **Logging**: Records runtime for each run to a CSV file with study metadata

## Usage Example

```bash
python scripts/scalability_worker.py speed-up 5 8 50 results.csv
```

This runs a speed-up study with 8 cores, using 50% of the data, repeated 5 times, with results saved to `results.csv`.

## Output

Results are appended to the specified CSV file with columns:
- `study_type`: Type of study
- `study_id`: Unique identifier for the study session
- `run_id`: Run number within the session
- `cores`: Number of cores used
- `percentage`: Percentage of data used
- `runtime`: Execution time in seconds

# 3) UK Police Stop and Search Analysis - Overview

The main notebook `uk_police_notebook.ipynb` performs a large-scale data analysis of **Stop and Search** records from the UK Police open data portal, focusing on bias detection and scalability analysis.

## Purpose
Analyze potential biases in police stop and search practices through: 
- Racial and demographic disparities analysis
- Machine learning pipeline development
- Scalability testing for big data processing
- Longitudinal feature importance tracking

---

## Structure

### 1. Introduction
- **Context**: Stop and Search powers granted to UK police officers
- **Data Source**: UK Police open data portal ([data.police. uk](https://data.police.uk/))
- **Objective**: Investigate systemic biases and build scalable ML models
- **Dataset**: ~1.45M records of stop and search incidents

---

### 2. Preliminary Exploratory Data Analysis (EDA)

#### 2.1 Data Dictionary
Comprehensive field descriptions from UK Police API: 
- **Demographics**: age_range, gender, self_defined_ethnicity, officer_defined_ethnicity
- **Operational**: type, datetime, legislation, object_of_search, outcome
- **Location**:  latitude, longitude, street information
- **Metadata**: force_id, operation details, outcome linkage

#### 2.2 Data Analysis

##### 2.2.1 Ethnicities Biases
**Visualizations include:**
- Yearly trends in search volumes
- Self-defined vs officer-defined ethnicity comparison
- Search type distribution (Person/Vehicle/Combined)
- Outcome analysis (arrests, no action, cautions, etc.)
- Legislative powers used (top 10)
- Geographic distribution by police force
- High-frequency search locations
- Gender distribution analysis
- Intersectional analysis:  gender ratios within ethnic groups

**Key Findings:**
- Significant disparities across ethnic groups
- Male individuals disproportionately searched
- Majority of searches result in "no further action"
- Variation across police forces and locations

---

### 3. Data Preprocessing

**Data Cleaning:**
- Filter to person-involved searches only
- Remove redundant columns (datetime after feature extraction, operation fields)
- Handle missing values

**Feature Engineering:**
- Temporal decomposition: year, month, day of year, day of week, hour
- Categorical encoding for ML readiness
- Target variable creation (likely binary arrest outcome)

**Rationale:**
- Optimize for Spark distributed processing
- Focus on bias-relevant features
- Prepare data for ML pipeline

---

### 4. Pipeline for ML Solving

Development of machine learning models with multiple validation strategies to handle:
- Class imbalance (most searches don't result in arrests)
- Temporal dependencies
- Model fairness across demographic groups

#### 4.1 Cross Validation Time Series
- Temporal split strategy to avoid data leakage
- Respects chronological order of events
- Prevents future data from influencing past predictions

#### 4.2 Cross Validator with Base Dataset
- Baseline model performance
- Standard cross-validation on original (imbalanced) data
- Establishes performance benchmarks

#### 4.3 Cross Validator with Weighted Classes
- Address class imbalance through weighting
- Penalize misclassification of minority class (arrests)
- Compare performance vs baseline

#### 4.4 Cross Validator with Balanced Dataset
- Undersampling/oversampling techniques
- Create balanced training sets
- Evaluate impact on bias metrics and accuracy

---

### 5. Scalability Study: Size-up, Scale-up and Speed-up

Systematic evaluation of system performance as data and resources scale. 

#### 5.1 Size-up
- **Objective**: How does performance change with increasing data volume?
- **Method**: Test with subsets (25%, 50%, 75%, 100% of data)
- **Metrics**: Training time, memory usage, model accuracy
- **Purpose**: Understand algorithmic complexity

#### 5.2 Speed-up
- **Objective**:  Does adding more processors improve performance?
- **Method**:  Vary number of Spark executors/cores
- **Metrics**: Wall-clock time, speedup factor, efficiency
- **Purpose**: Evaluate parallelization effectiveness

#### 5.3 Scale-up
- **Objective**:  Can we handle larger datasets by adding proportional resources?
- **Method**:  Increase data size and computational resources together
- **Metrics**: Consistent execution time, resource utilization
- **Purpose**: Test horizontal scalability

---

### 6. Scalability Study Analysis

Comprehensive evaluation of the scalability experiments. 

#### 6.1 Size-up Study Results
- Performance curves as data size increases
- Identification of bottlenecks
- Memory and computational limits
- Algorithm complexity validation (O(n), O(n log n), etc.)

#### 6.2 Speed-up Study Results
- Speedup factor vs number of processors
- Parallel efficiency metrics
- Identification of diminishing returns
- Amdahl's Law validation

#### 6.3 Scale-up Study Results
- System behavior under proportional scaling
- Resource utilization patterns
- Cost-effectiveness analysis
- Scalability ceiling identification

#### 6.4 Scalability Study Summary
- Overall system scalability assessment
- Recommendations for production deployment
- Optimal resource allocation strategies
- Cost-benefit analysis

---

### 7. Longitudinal Feature Importance Analysis

Track how feature importance changes over time to detect shifting bias patterns.

#### 7.1 Function Definitions
- Custom functions for temporal analysis
- Feature importance extraction methods
- Visualization utilities
- Statistical significance tests

#### 7.2 Main Code
- Time-windowed model training
- Feature importance tracking across periods
- Comparative analysis (year-over-year, seasonal)
- Identification of emerging bias patterns

#### 7.3 Conclusion
- Summary of temporal trends in feature importance
- Evidence of systematic vs sporadic biases
- Policy implications and recommendations
- Limitations and future work

---

## Technical Stack
- **PySpark**: Distributed data processing and MLlib
- **Pandas**:  Aggregated data visualization
- **Matplotlib/Seaborn**: Statistical visualizations
- **Data Format**: Parquet (columnar storage)
- **ML Models**: Decision Trees (implied from context)

## Dataset Statistics
- **Total Records**: 1,451,297
- **Person-Involved**: 1,403,329 (96.7%)
- **Time Period**: Multi-year coverage (2015-2025)
- **Geographic Coverage**: All UK police forces
- **Features**: 23 original → ~15 engineered features

## Key Research Questions
1. **Bias Detection**: Are certain demographic groups disproportionately targeted?
2. **Model Fairness**: Can we build unbiased predictive models?
3. **Scalability**: Can this analysis scale to national/international datasets?
4. **Temporal Patterns**: How do biases evolve over time? 
5. **Resource Optimization**: What's the optimal infrastructure for this analysis? 
6. **Policy Impact**: What interventions could reduce disparities?

## Methodological Innovations
- **Time-series aware cross-validation** for temporal data
- **Multi-strategy imbalance handling** (weighting + sampling)
- **Comprehensive scalability study** (size/speed/scale-up)
- **Longitudinal feature tracking** for bias evolution
