This project is meant to help the music app Sparkify to move their date warehouse to a data lake.
It involved 3 steps:
- Extract the data from S3
- Processes them using Spark into set of dimensional tables.
- Load them back to S3
This will allow analytics team to find insights in what songs their users are listening to.

Dataset
Datasets used were provided in two public S3 buckets as JSON files.
- One bucket contains info about songs and artists
- The second has info about actions done by users, which were stored as logs

