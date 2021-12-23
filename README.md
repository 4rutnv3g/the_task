# THE Data Engineer Task

## Installation

- Install Python 3.8 or above & `pip`
- `pip install -r requirements.txt`

## Running

- Raw JSON data should be placed in `data/raw` (the data provided with the task is already there)
- From repository root, run `python stc/etl.py` to load JSON files and process to Parquet
- From repository root, run `python stc/db.py` to start a DuckDB instance, create some views to query Parquet data and
  open a SQL shell
    - the pre-created views include basic `institutions`/`submissions`/`subjects` in addition to some analysis views
      presenting summary data
    - the SQL shell accepts most normal ANSI SQL commands

## Notes

The original brief encourages where possible to follow my usual design and development process. The modern data
engineering stack consists of cloud-based managed services so this was not strictly possible. Nonetheless I have made
some attempts to build the local solution in a way that simulates certain elements of the modern cloud data stack, e.g.
keeping the processed data in files and using an in-process query engine rather than using a database to simulate
separation of storage and compute.

I have made a few notes below about how to develop this work further; crucially containerisation and a simpler interface
would be essential developments but a little out of scope here. I have also mentioned deployment; it is worth noting
that I would not advocate deploying this particular solution; if I had plans to deploy this to the cloud it would be
done a totally different way using a cloud-native stack.

The SQL for creating the views can be viewed in `src/initialise.sql` and `src/analyse.sql`

### Future Work

- clearer/better interface
- incrementally load submissions files
    - allow ingestion of new submissions after initial load
- much more flexibility in general
- containerise
- deploy
- proper git practices
- logging level configurable
- summary metrics??
- catch and log errors
- more dates
    - annual
    - year vs previous year
