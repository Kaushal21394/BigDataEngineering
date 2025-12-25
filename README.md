# Big Data Engineering Portfolio

This repository is a **Big Data Engineering portfolio** showcasing hands-on projects built with **Hadoop MapReduce, Hive, Pig, and MongoDB**. The work focuses on **distributed batch analytics**, **Hadoop programming patterns**, **custom Writable/Partitioner implementations**, and **NoSQL aggregation** using real-world datasets such as **Airline On-Time Performance**, **NYSE market data**, and **MovieLens**.

---

## 🔧 Tech Stack & Skills

- **Languages:** Java, Pig Latin
- **Big Data:** Hadoop MapReduce
- **Query / ETL:** Hive, Pig
- **NoSQL:** MongoDB
- **Key Concepts:**
  - Multi-stage MapReduce pipelines (job chaining)
  - Reduced-side joins and partitioned aggregation
  - Custom `Writable` / composite keys and secondary-sort patterns
  - Input formats (Sequence files, fixed-length records, combined file input)
  - Performance optimizations using combiners
  - MongoDB MapReduce + Java integration

---

## 📂 Repository Projects

### 1) Advances Hadoop Algorithms
📁 **Path:** `Advances Hadoop Algorithms/`

Advanced Hadoop programming patterns and algorithmic implementations using **Java MapReduce**.

**Contents**
- **Job chaining:** `Code/hw4_chaining/`  
  Multi-stage MapReduce pipeline using multiple mappers/reducers (First/Second Mapper & Reducer).
- **NYSE average with composite keys:** `Code/hw4_nyse_avg/`  
  Implements averaging with a `CompositeKeyWritable` (useful for ordering/secondary sort style patterns).
- **NYSE max per symbol:** `Code/nyse_homework4/`  
  Computes maximum stock price per symbol using custom keys and reducers.

📄 Includes assignment documentation: `Assignment 4.pdf`

---

### 2) Airline On-Time Performance (End-to-End Analytics)
📁 **Path:** `Airline On time Performance/`

A complete analytics project using **MapReduce + Hive + Pig** over airline on-time performance data.  
Includes a full project writeup: `Chaudhary_Kaushal_Project_Report.pdf`

#### ✅ MapReduce Modules
📁 **Path:** `Airline On time Performance/MapReduce/`

Projects include:
- **Best Airport per Year:** `Project_BestAirportPerYear/`  
  Includes custom writable + partitioning + join patterns.
- **Best Time to Fly:** `Project_BestTimeToFly/`  
  Analyzes traffic patterns across daily/weekly/monthly/yearly dimensions.
- **Cancellation Analysis:** `Project_CancellationAnalysis/`  
  Computes cancellation counts and bins cancellations by reason (carrier/weather/NAS/security/unknown).
- **Percentage Delay per Year:** `Project_PercentageDelayPerYear/`  
  Calculates delay percentage metrics by year.
- **Top 10 Routes with Most Delays:** `Project_Top10RoutesWithMostDelays/`  
  Finds and ranks routes by delay metrics.
- **Utility - putMerge:** `putMerge/`  
  Helper project to merge multiple output files (common in Hadoop workflows).

📁 Output samples are included under:  
`Airline On time Performance/MapReduce/mapreduceOutputData/`

#### 🐝 Hive (ETL + Queries)
📁 **Path:** `Airline On time Performance/Hive/`

- Hive ETL scripts and query sets
- Includes sample Hive output/input data under `hiveInputData/`

#### 🐷 Pig (Batch Analytics)
📁 **Path:** `Airline On time Performance/PIG/`

Pig scripts and outputs for:
- Delay by weather
- Total delay by carrier
- Routes with most diversions
- Top airports with minimum delay

Outputs included under:  
`Airline On time Performance/PIG/pigOutputData/`

---

### 3) MongoDB 101 (Fundamentals + Java + Datasets)
📁 **Path:** `Mongo DB 101/`

MongoDB basics plus hands-on integration with **Java** and datasets.

**Highlights**
- Java source (MovieLens collections): `MongoDB/src/movielens/`
- Datasets included under: `MongoDB/data/`
  - `ml-10M100K/` (MovieLens)
  - `NYSE/` CSV dataset slices

📄 Includes assignment documentation: `Assignment 1.pdf`  
📝 Supporting notes/scripts: `mr codes.txt`

---

### 4) MapReduce on MongoDB
📁 **Path:** `MR on Mongo DB/`

MongoDB MapReduce-focused work:
- `mr_codes.txt`
- Documentation: `Map Reduce on Mongo DB.pdf`

---

### 5) NYSE Hadoop Program
📁 **Path:** `NYSE Hadoop Program/`

A multi-part Hadoop program exploring different data processing patterns using **NYSE data** and other datasets.

📄 Includes assignment documentation: `Assignment_3.pdf`

**Hadoop Programs**
📁 `NYSE Hadoop Program/Hadoop_Programs/`

- **Part 3 — Access Logs:** `Part 3/AccessLogs/`  
  IP-based aggregation using MapReduce.
- **Part 4 — NYSE Max Price + putMerge:** `Part 4/NYSE_max_price/`, `Part 4/putMerge/`
- **Part 5 — Advanced I/O and Analytics**
  - Fixed-length records: `Part 5/fixedLengthRecords/`
  - Average stock price + combiner + combined input format: `Part 5/NYSE_avg_stock_price/`
  - Ratings analysis: `Part 5/ratings/`
  - Sequence file input: `Part 5/SeqFile_input/`
  - Sequence file creator: `Part 5/SequenceCreator/`
  - Gender aggregation example: `Part 5/Gender/`

---

## ▶️ How to Run (General)

Most MapReduce modules are Maven-based Java projects.

```bash
# build
mvn clean package

# run (example pattern)
hadoop jar target/<jar-name>.jar <MainClass> <input_path> <output_path>
```

## 📫 Contact

- GitHub: https://github.com/Kaushal21394
- LinkedIn: linkedin.com/in/kaushal-chaudhary
- Email: kaushal.chaudhary.1994@gmail.com

---

## License
This repository is licensed under **GPL-3.0** (see `LICENSE`).
