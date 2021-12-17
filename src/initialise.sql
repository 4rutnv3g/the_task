create view institutions as
select
    *
from
    './data/bucket/institutions.parquet'
;

create view subjects as
select
    *
from
    './data/bucket/subjects.parquet'
;

create view submissions as
select
    *
from
    './data/bucket/submissions.parquet'
;

