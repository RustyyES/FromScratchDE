# 🚀 Data Engineering From Scratch

![Project Status](https://img.shields.io/badge/Status-Completed-success)
![Python](https://img.shields.io/badge/Python-3.x-blue)
![Dependencies](https://img.shields.io/badge/Dependencies-Zero-green)

Welcome to **Data Engineering From Scratch**! 🎓

This is an educational project where I rebuilt the core components of the modern Data Engineering stack (**Spark**, **Airflow**, **Kafka**, **Requests**, **GreatExpectations**) using **Pure Python**. No external libraries (except standard libs) were used for the core logic.

The goal was to demystify how these tools work under the hood by implementing their fundamental algorithms: **MapReduce**, **DAG Topological Sort**, **Commit Logs**, **Token Buckets**, etc.

## 🏗️ Architecture

The project is organized into modular "Mini" components, each simulating a real industrial tool:

| Component | Industry Equivalent | Key Features Implemented |
| :--- | :--- | :--- |
| **`spark_like`** | Apache Spark | Lazy Evaluation, Distributed Map/Filter/Reduce, Partitioning, shuffles. |
| **`airflow_like`** | Apache Airflow | DAGs, Topological Sort, Scheduler, Sensors, Branching, SequentialExecutor. |
| **`kafka_like`** | Apache Kafka | Topics, Partitions, Offset Management, Consumer Groups, Stream Windowing. |
| **`api_like`** | Requests / Limits | Retry Logic, Token Bucket Rate Limiter, Pagination. |
| **`quality`** | GreatExpectations | Expectations Suite, Data Validation Engine. |

---

## 📅 Curriculum & Features

### Week 1-2: Distributed Compute (Mini-Spark)
- Implemented **Lazy Execution** (transformations aren't computed until an action is called).
- Created a **Partitioning System** to split data across "nodes" (simulated via Threads).
- Built **MapReduce** primitives (`map`, `filter`, `reduce_by_key`).
- **Demo**: Processing a large CSV with lazy loading.

### Week 3-4: Workflows (Mini-Airflow)
- Built a **DAG Engine** that resolves task dependencies using **Kahn's Algorithm**.
- Implemented **Deterministic Prioritization** (tasks with higher priority run first).
- Added **Sensors** (`FileSensor`, `TimeSensor`) and **State Management** (JSON backend).
- **Demo**: A pipeline that waits for a file trigger before execution.

### Week 5-6: Streaming (Mini-Kafka)
- Implemented a **Commit Log** architecture (Topics & Partitions).
- Built **Producers** (hashing keys to partitions) and **Consumers** (polling offsets).
- Added **Consumer Groups** with round-robin load balancing.
- Implemented **Tumbling Windows** for stream aggregation.

### Week 7-8: Reliability & Quality
- **API Simulator**: A resilient HTTP client with **Rate Limiting** (Token Bucket) and Auto-Pagination.
- **Data Quality**: A validation framework to ensure data integrity (Null checks, Range checks).

---

## ⚡ Performance Benchmark: The Grand Finale

To verify the architecture, I ran an End-to-End Benchmark using **real-world data** from the **USGS Earthquake API**.

**The Pipeline:**
1.  **Airflow**: Orchestrate the workflow.
2.  **API Client**: Fetch last week's earthquake events (resiliently).
3.  **Kafka**: Ingest 1,700+ events and partition them by region.
4.  **Spark**: Perform a "heavy" distributed aggregation to find the most active regions.

**The Results:**
I compared the **Mini-Spark (4 Partitions)** implementation against a **Single-Threaded Baseline**.

| Engine | Processing Time | Speedup |
| :--- | :--- | :--- |
| **Single-Threaded Baseline** | 0.73s | 1.0x |
| **Mini-Spark (4 Threads)** | **0.25s** | **2.92x 🚀** |

*Note: The speedup comes from parallel execution of partitions using `ThreadPoolExecutor`. This mimics how real clusters scale!*

**Insights Found:**
Top active earthquake regions (Magnitude > 4.0):
1.  **Russia** 🇷🇺
2.  **Indonesia** 🇮🇩
3.  **Philippines** 🇵🇭

---

## 🛠️ How to Run

Clone the repo and run the examples. No `pip install` required for the core!

### 1. Run the Grand Finale Benchmark
```bash
python3 data_engineering_from_scratch/examples/usgs_benchmark.py
```
*Watch Airflow execute the DAG tasks step-by-step!*

### 2. Run Spark Demo
```bash
python3 data_engineering_from_scratch/examples/demo_real_data.py
```

### 3. Run Kafka Streaming Demo
```bash
python3 data_engineering_from_scratch/examples/example_kafka_phase3_2.py
```

### 4. Run Data Quality Check
```bash
python3 data_engineering_from_scratch/examples/example_quality_phase5.py
```

---

## 🧠 Lessons Learned
- **Lazy Evaluation** is huge for memory saving but makes debugging tricky.
- **Partitions** are the secret sauce of scalability.
- **Topological Sorting** is just fancy dependency resolution.
- **Distributed Systems** are hard to simulate perfectly on one machine, but Threading gets us close!

---
*Created by Eiad as part of the Data Engineering Study Track.*
