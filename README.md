# wikipedia-analysis
Computation of the ranking of several programming languages using their occurrences in Wikipedia pages.

Gauging how popular a programming language is important for companies judging whether or not they should adopt an emerging programming 
language. For that reason, industry analyst firm RedMonk has bi-annually computed a ranking of programming language popularity 
using a variety of data sources, typically from websites like GitHub and StackOverflow. See their [top-20 ranking for June 2016](http://redmonk.com/sogrady/2016/07/20/language-rankings-6-16/) as an example.

Here we'll use our full-text data from Wikipedia to produce a rudimentary metric of how popular a programming language is, 
in an effort to see if our Wikipedia-based rankings bear any relation to the popular Red Monk rankings.

## Getting the data

You can download the necessary data by clicking [here](https://drive.google.com/file/d/0B1SO9hJRt-hgbkYxdVM5bmdRT0E/view?usp=sharing). Place it inside `src/main/resources/wikipedia`.

## Running the code

Just run `sbt run` on your console.

## TODO

 * Add more unit tests.
