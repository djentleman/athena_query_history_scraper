# athena_query_history_scraper
Little tool to scrape the query history of AWS athena.

## Usage

```
# without argments it will scrape the most recent 500 queries for each workgroup
python scrape_athena_query_history.py
# you can pass in the total query count as an argument
python scrape_athena_query_history.py 10000
```

results are output here: `athena_scraped_results.csv`
