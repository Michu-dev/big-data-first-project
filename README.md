<h1 align="center">
    MapReduce, Hadoop Streaming, Pig, Hive - project
</h1>

## About
The main goal of the project was to analyse datasets avialable on: http://www.cs.put.poznan.pl/kjankiewicz/bigdata/projekt1 or https://www.imdb.com/interfaces/ and get the results for 3 genres among feature films (titleType=movie) with the most engaged actors. Final result present 3 columns: genre, number of available films of that genre and number of actors who played a role in these films. The first task of the flow was to analyse `title.principals.tsv` file using `mapreduce` in the classic approach written in `Java` and count number of actors for every film. The second task was to use `Hive` platform to generate final result based on mapreduce task result and `title.basics.tsv` file in the JSON format.


## Project Structure
Folder `MapReduceProject` contains the Java project for MapReduce task, `analyse_films.hql` script to create final result and `solution_script` which runs all the tasks.

## Running the project
The project was created on `Google Cloud Platform`. To run the analyse it is necessary to load input data and builded artifact in .jar format from `MapReduceProject` into a bucket and load into running cluster. Then you have to put data into input folder to hdfs and modify paths in `analyse_films.hql` file. At the end you have to just run the `solution_script` which creates final result.