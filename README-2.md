
# MC Market Share Predictor


## Market Center Market Share Predictor

This project predicts how market centers will perform based on inputs, then analyzes market center inputs from their financials to explain performance.


## Data Pipeline

Creates the final analysis table by querying the necessary data from the source tables. 

The finaly query used to create this table can be found in 
```bash 
src/bigquery/master_analysis_table.sql
```
There are other queries saved in the bigquery folder that form the inputs to that last master query.


## Final Data Set for Statistical Analysis

The datase for the final statistical analysis can be found here: 

```bash
src/data/master_analsyis_table.csv

```


