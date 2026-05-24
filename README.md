# IPL Data Engineering & Analytics Project

This project is an end-to-end IPL analytics platform built on AWS using a layered data lake architecture. The goal of the project is to transform raw IPL match and ball-by-ball data into meaningful analytical datasets that can be used to analyze player performance, match trends, MVP rankings, batting consistency, and head-to-head statistics.

The project uses AWS Glue and PySpark for ETL processing, Amazon S3 for storage, and Amazon Athena for querying the final datasets.

## Project Architecture

Raw Data → Silver Layer → Gold Analytics Layer

### Silver Layer
The silver layer contains cleaned and standardized IPL datasets including:

- Match data
- Ball-by-ball delivery data

Data cleansing steps include:
- Duplicate removal
- Data type standardization
- Data validation
- Dataset joins for analytics processing

### Gold Layer

The gold layer contains business-ready analytical tables.

#### Player Analytics

### gold_player_career_stats
Career-level batting statistics including:
- Runs
- Matches
- Balls faced
- Fours
- Sixes
- Strike rate
- Batting average

### gold_player_season_stats
Season-wise batting statistics including:
- Runs per season
- Strike rate
- Batting average
- Boundary statistics
- Seasonal performance trends

#### MVP Analytics

### gold_mvp_points_career
Career MVP rankings based on:
- Boundaries
- Sixes
- Fifty bonuses
- Hundred bonuses

### gold_mvp_points_season
Season-wise MVP rankings and player impact analysis.

#### Phase Analysis

### gold_phase_analysis
Analysis of scoring patterns across:
- Powerplay overs
- Middle overs
- Death overs

### gold_innings_phase_analysis
Innings-level phase performance analysis to identify scoring trends and match strategies.

#### Player Recognition Analytics

### gold_player_of_match_stats
Player of the Match achievements and overall match-winning impact.

### gold_player_of_match_season
Season-wise Player of the Match performance tracking.

#### Advanced Batting Analytics

### gold_batsman_vs_bowler
Head-to-head batting performance against individual bowlers including:
- Runs scored
- Balls faced
- Strike rate
- Dismissals

### gold_batsman_vs_team
Career performance against opposition teams while preserving franchise context. Metrics include:
- Runs
- Balls faced
- Fours
- Sixes
- Matches
- Strike rate
- Batting average
- 50s
- 100s

### gold_batsman_vs_team_season
Season-wise performance against opposition teams with detailed matchup analysis.

#### Consistency Analytics

### gold_consistency_score
Player consistency measurement based on season and match performance to identify the most reliable batsmen.

## Technologies Used

- AWS S3
- AWS Glue
- PySpark
- Amazon Athena
- Python
- Parquet
- Git
- GitHub

## Key Concepts Implemented

- ETL pipeline development
- Data lake architecture
- Data cleansing and transformation
- Multi-stage aggregations
- Cricket analytics metrics
- Match-level and season-level analysis
- Career-level analytics
- Head-to-head performance analysis
- Data validation using Athena
- Parquet-based storage optimization

## What I Learned

Through this project I gained hands-on experience in building production-style data pipelines on AWS. I worked extensively with PySpark transformations, aggregation logic, data modeling, S3-based data lakes, Athena queries, and analytical dataset design. The project also helped me improve my understanding of cricket analytics and how business requirements can be converted into scalable data engineering solutions.

## Project Outcome

The final solution provides a scalable IPL analytics platform capable of supporting player analysis, matchup analysis, MVP rankings, consistency measurement, season comparisons, and phase-wise performance tracking. The project demonstrates practical data engineering skills including AWS Glue ETL development, data modeling, analytical table design, and cloud-based data processing.
