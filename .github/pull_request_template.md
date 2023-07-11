## Checklist for the Refactoring Project

<!-- If you are done with a topic mark the checkboxes with an `x` (like `[x]`) -->

- [x] I read and understood the tasks.
- [x] I created a script that loads the data.
- [x] I updated the script to upload the data to GCS.
- [x] I extract the data from the GCS.
- [ ] ETL:
    - [ ] I created a script that is processing the data.
    - [ ] I created a script that is calculating the revenue per day.
    - [ ] I created a script that is loading the data into Postgres/BigQuery.
- [x] ELT:
    - [x] I created a script that is loading the data into Postgres/BigQuery.
    - [ ] I created a dbt models that are processing the data.
    - [ ] I created a dbt model that is calculating the revenue per day.

Bonus:
- [ ] I used prefect for the Workflow Orechestration.
