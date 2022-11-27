# Project: Data Lake
This project is concerned with the modeling of an *analytical database* for the data analytics team of the fictional music and audio streaming startup *Sparkify LLC*.
The analytics team is interested in different business related-questions, such as what songs the individual users are listening to, in order to make them recommendations for new songs and artists.

The project involves two tasks: The first is the design of a database which fulfils the analytical teams requirements. The second is the creation of a ETL-process which takes the raw data and transforms and inserts it into the newly created analytical database.

The raw data consists of two JSON-formatted document-based databases, one in which information about songs and artists is stored, and one which contains the logs of the customers' songplays.

Within this project, the target database will be loaded in a python script raw by raw using `for`-loops and SQL-`INSERT` statements. The main data engineering tool here, apart from `Python`, is [Pandas](https://pandas.pydata.org/). The implemented ETL-process is slow and cumbersome, but we will learn more efficient methods along the course of the data engineering nanodegree program. 



## Quick Start

The ETL-process can be started by launching:
```console
foo@bar:~$ python etl.py
```
Please make sure that python (>=3.6.3) is installed.


## Purpose of the Analytical Database
The purpose of the analytical database and hence the corresponding ETL process is to enable the Sparkify's data analysis team to do fast and efficient queries about business related questions in a flexible manner.

The songplay history of a user can be easily extracted from the fact table `fact_table`, and ranked using their respective number of plays. We interpret a song as highly liked by the user if the song has been played a high number of times, in relative terms (cf. example queries below).

This result can be used for a recommender system: If the most liked songs of one user match the most liked songs of another user, measured by an appropriate metric, then also other songs liked by one user may be recommended to the other user. 

A standard example of such a [recommender system](https://en.wikipedia.org/wiki/Recommender_system) is the use of [cosine similarity](https://en.wikipedia.org/wiki/Cosine_similarity).

Furthermore, the database shall provide enough flexibility for Sparkify's analytical team to perform fast and spontaneous queries for other business-cases, such as finding songs to promote to a wide audience, do analytical queries on the demographics of the listeners etc.



## ETL-Pipeline
The final ETL-pipeline which populates the analytical database is implemented in `etl.py` and consists of stages:

 1. Process raw song data,
 2. Process raw log data.

In the first stage, the song data is imported using `Pandas` and converted to a Python-`list`, selecting only a subset of five attributes which are necessary to fill `songs_dim`. The same is done with `artists_dim`, selecting also a subset of five attributes from the raw data and inserting into the table using a `for`-loop.

The second stage is more interesting. Firstly, the tables `time_dim` and `user_dim` are being populated. In order to populate `time_dim`, the timestamp `ts` from the log data is extracted and transformed using Pandas into the appropriate tuple 
```
('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
```
Then, we insert `user_dim` in the direct way again, only selecting the specified columns from the log data, as we have done in the two dim-tables in the first stage. 

Finally, `fact_songs` is being populated by making an inner join of `artists_dim` with `songs_dim` on the common attribute `artist_id`.



## Repository
This repository contains the following files and directories:

```
├── data/
│   ├── log_data
│   ├── song_data
│   ├── log_data.zip
│   └── song_data.zip
├── output/
├── images/
├── dl.cfg
├── etl.ipynb
├── etl.py
└── README.md
```

 - `data/`: Contains a subset of the raw data set for local experimentation.

 - `output/`: A target folder for the output, when experimenting locally.

 - `dl.cfg`: Contains the AWS credentials.

 - `etl.ipynb`: The Jupyter notebook file which is used for discovery and experimentation.

 - `etl.py`: Reads data from S3, processes the data using Spark, and writes them back to S3.

 - `README.md`: The file you are currently reading.