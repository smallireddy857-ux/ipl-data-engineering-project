# IPL Data Engineering & Analytics Project

This project is an end-to-end IPL analytics platform built on AWS using a layered data lake architecture. The main objective of the project was to take raw IPL match and ball-by-ball data, process it using ETL pipelines, and create analytical datasets that can be used to understand player performance, scoring patterns, consistency, MVP rankings, Player of the Match achievements, and head-to-head matchups.

The project was developed using AWS Glue and PySpark for data processing, Amazon S3 for storage, and Amazon Athena for querying and validating the final datasets.

## Architecture

Raw IPL Data → Silver Layer → Gold Analytics Layer

### Silver Layer

The silver layer contains cleaned and standardized IPL datasets including:

- Match data
- Ball-by-ball delivery data

The data preparation process includes:

- Removing duplicate records
- Standardizing data types
- Data quality checks
- Joining match and delivery datasets
- Preparing data for downstream analytics

### Gold Layer

The gold layer contains business-ready analytical tables designed for reporting and player performance analysis.

---

## Gold Analytics Tables

### gold_player_career_stats

Career-level batting statistics for every batsman.

Metrics:
- Total Runs
- Balls Faced
- Matches Played
- Fours
- Sixes
- Strike Rate

---

### gold_player_season_stats

Season-wise batting statistics for every batsman.

Metrics:
- Season Runs
- Balls Faced
- Matches Played
- Fours
- Sixes
- Player of the Match Awards
- Strike Rate

---

### gold_mvp_points_career

Career-level MVP points calculated for each batsman.

Metrics:
- Career MVP Points

Dimensions:
- Batsman

---

### gold_mvp_points_season

Season-wise MVP point analysis for each batsman.

Metrics:
- Season MVP Points

Dimensions:
- Season
- Batsman

---

### gold_phase_analysis

Batting performance analysis across different phases of an innings.

Metrics:
- Total Runs
- Balls Faced
- Strike Rate

Dimensions:
- Season
- Batting Team
- Batsman
- Phase

The phase analysis helps identify how players perform during different stages of an innings such as powerplay, middle overs, and death overs.

---

### gold_innings_phase_analysis

Innings-level phase analysis that provides a more detailed breakdown of batting performance within individual innings.

Metrics:
- Total Runs
- Balls Faced
- Strike Rate

Dimensions:
- Match ID
- Innings
- Season
- Batting Team
- Batsman
- Phase

---

### gold_player_of_match_stats

Career-level Player of the Match achievements.

Metrics:
- Total Awards Won
- Seasons Won

Dimensions:
- Player of Match

---

### gold_player_of_match_season

Season-wise Player of the Match performance.

Metrics:
- Awards Won

Dimensions:
- Season
- Player of Match

---

### gold_batsman_vs_bowler

Head-to-head batting performance against individual bowlers.

Metrics:
- Runs Scored
- Balls Faced
- Fours
- Sixes
- Strike Rate

Dimensions:
- Batsman
- Bowler

This table can be used to identify favorable and unfavorable player matchups.

---

### gold_batsman_vs_team

Career batting performance against opposition teams while preserving franchise context.

Metrics:
- Career Runs
- Career Balls
- Career Fours
- Career Sixes
- Dismissals
- Matches
- Fifties
- Hundreds
- Strike Rate
- Batting Average

Dimensions:
- Batsman
- Batsman Team
- Opposition Team

This design ensures that players who represented multiple franchises are analyzed correctly without mixing statistics across teams.

---

### gold_batsman_vs_team_season

Season-wise batting performance against opposition teams.

Metrics:
- Season Runs
- Season Balls
- Season Fours
- Season Sixes
- Dismissals
- Matches
- Fifties
- Hundreds
- Strike Rate
- Batting Average

Dimensions:
- Season
- Batsman
- Batsman Team
- Opposition Team

---

### gold_consistency_score

Batting consistency analysis based on average runs and a calculated consistency score.

Metrics:
- Average Runs
- Consistency Score

Dimensions:
- Batsman

This table helps identify players who consistently contribute across matches rather than relying on a few high-scoring performances.

---

## Technologies Used

- AWS S3
- AWS Glue
- PySpark
- Amazon Athena
- Python
- Parquet
- Git
- GitHub

---

## Key Concepts Implemented

- ETL Pipeline Development
- Data Lake Architecture
- Data Cleansing and Transformation
- PySpark DataFrame Operations
- Match-Level Aggregations
- Season-Level Aggregations
- Career-Level Analytics
- Head-to-Head Analysis
- Phase-Based Cricket Analytics
- Strike Rate Calculations
- Batting Average Calculations
- Consistency Scoring
- MVP Point Calculations
- Player of the Match Analytics
- Data Validation with Athena
- Parquet-Based Storage Optimization

---

## What I Learned

Through this project, I gained practical experience building data pipelines on AWS using Glue and PySpark. I worked with large datasets, performed transformations and aggregations, designed analytical tables, validated outputs using Athena, and implemented a layered data lake architecture on Amazon S3.

The project also helped me improve my understanding of cricket analytics by converting raw match events into meaningful business-focused datasets that can be used for player performance analysis and reporting.

---

## Project Outcome

The final solution provides a scalable IPL analytics platform capable of supporting player analysis, season comparisons, batting consistency measurement, MVP rankings, phase-wise scoring insights, Player of the Match tracking, and detailed head-to-head performance analysis.

This project demonstrates practical data engineering skills including AWS Glue ETL development, PySpark transformations, analytical data modeling, cloud-based data processing, and data lake implementation on AWS.
