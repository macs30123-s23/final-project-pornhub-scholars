# Tracking Sexism in Pornhub Comments
## Pornhub Scholars: Kaya Borlase, Joseph Helbing, Loizos Bitsikokos
### MACS 30123
### Winter 2023

## Introduction, Background and Significance

In the digital age, there is a tremendous surplus of sexual content online, while visibility of sex workers (and their rights) is becoming increasingly relevant. The amount of online activity around sexual content has the potential to have real-world, particularly harmful implications, as the lives of sex workers could be exploited or endangered. 

Online pornographic content has also severe implications for how content consumers perceive women (note that the term women in this context is used to refer to women regardless of their sex assigned at birth). Sexual online content and the public discourse around it in platforms offering pornographic content is severely understudied (Phihlaja, 2016), while the only computationally significant studies of such content are conducted by the platforms themselves (see for example [Pornhub comments](https://www.pornhub.com/insights/pornhub-comments)). This project explores the perception of sex workers online by conducting a content analysis on comments scraped from Pornhub, a pornographic video-streaming website (see this [Wiki Article](https://en.wikipedia.org/wiki/Pornhub) for more information). 

We built a large-scale web-scraping and data pipeline on AWS that scraped comments from videos. We additionally conducted a data exploration and sentiment analysis to gain a general perception of the comment space. Finally, we trained sexism detector models to evaluate the amount of sexism prevalent in comments towards sex workers online. Overall, our research answers the following research questions: 

1. What is the public discourse in online pornography websites?
2. To what extent is there sexism in these media? Is the content positive? Negative? Or even nasty?

Sex work has significant consequences for social life that often go unoticed. To showcase the hidden implications sex work can have in society, we conducted a short ethnographic study. we interviewed Greece's last model shoe maker, who constructs shoe models for both large-scale production and custom made shoes. With his business slowly fading away, he largely depends on custom shoe model orders by workers in the sex industry. The interview can be found [here](https://youtu.be/kgSEouqwGbw)


## Data, Methods and Scalability

As previously stated, we scraped the top comments under videos posted to the online pornography site, PornHub. We gathered a total of 839.537 comments, from 149.431 videos posted by 2.493 creators. After cleaning the comments by removing stop words, we proceeded to the analysis. Overall, because we have a fairly large dataset of text data, it made sense to use scalable computing at each phase of the project. In addition, since the website's content is expected to aggregate over time, it is important to have a working large-scale pipeline that scrapes content asynchronously and stores data on the cloud. Completing our analysis in a scalable way, enables us to continue studying these online spaces over time and enrich the literature even further.

To create a random selection of video comments to scrape, we made use of Pornhub's random video call feature, which surfaces a random video from the Pornhub library. The Pornhub website makes extensive use of Javascript, and so a maximum of 10 of the most popular (most upvoted) comments are available per video. Future research could make use of more advanced webscrapping libraries to further enrich our data. An explanation and justification of each method is presented below. 

### Sentiment classification

### Sexism classification

## Structure of Processes

To collect the data, we implemented a parallelized scraper using a lambda function via AWS. Because we wanted to scrape comments from a large number of videos, it was necessary to parallelize the scraping process. Additionally, our lambda function wrote the comments directly into an [Amazon RDS](https://aws.amazon.com/rds/) so we did not have to worry about a memory bottleneck if we transfered the data back to our local machine. As the lambda function used unincluded packages, a zip file with the required packages is stored and updated within an S3 bucket to pass the zip file to the lambda function. To run a large set of lambda functions, an SQS Queue with a lambda trigger was created.

## Environment Creation

Assuming the user has updated their AWS credentials, a script to create the required environment to run all actions for the study can be run via the [from_the_top.sh](from_the_top.sh) shell script. If the user does not have the micromamba package manager, the shell script can download and install micromamba via the --mamba flag otherwise the script will use the users existing micromamba installation.
```
bash from_the_top.sh --mamba
```

The script creates a virtual environment from the `environment.yml` file, creates the AWS relational database and S3 bucket, uploads the lambda zip file, creates the SQS Queue and lambda trigger, updates the lambda function from the S3 hosted zip and sends a number of lambda instantiations to the SQS Queue with the number of pages to scrape per lambda function.

## Data Collection

 Overall, our scraper runs via the following steps:

1. The database and S3 bucket were initialized in [create_database.py](create_database.py) and the information for the RDB and S3 bucket are saved in the configuration file `db_details.ini` via the CLI command
```
python create_database.py --create
```
2. The file `scrape.py` reads out the configuration information in `db_details.ini` and creates the SQS Queue and Lambda triggers, unzips the `lambda_deployment.zip` file, inserts the updated `lambda_function.py`, rezips the file and uploads it to the S3 bucket, then tells lambda to update or create the lambda function via the --update flag. The number of lambdas is controlled via -l flag and the number of pages per lambda is controlled via the -n flag.
```
python scrape.py --update -l 1000 -n 20
```

This scraper allowed us to scrape and store a large number of comments that we likely wouldn't have been able to scrape in a reasonable time frame on our local machines. We recognize that the more comments we have, the more likely it is that we have a dataset that accurately represents the comments that product consumer we will be able to get a dataset that represents the way that content consumers of pornographic material are reacting to female sex workers online.

An initial problem we encountered was maxing out the number of connections available in the MySQL relational database which caused connection errors. We were able to fix this issue by limiting the number of concurrent lambda functions in `scrape.py` to 20 and increasing the RDB to a T3.medium instance with 4GB of RAM which allowed us to increase the maximum connections from 306 to 600. 12,000 lambda functions pushed to the SQS Queue with a maximum concurrent lambda limit of 20 takes around 14 hours to complete a full run pulling roughly 900,000 comments, though this number will vary because of the call to Pornhub's random video call, which surfaces a random video from the Pornhub library.

When scraping is finished, the full database can be downloaded as set of 3 .parquet files via
```
python create_database.py --download
```
and the RDB can be closed via
```
python create_database.py --close
```

Code for [create_datapase.py](create_database.py) and [scrape.py](scrape.py).

## Data Exploration

In order to explore the data, we use Pyspark to visualize different aspects of the text data. Because Pyspark uses a [lazy evaluation model](https://data-flair.training/blogs/apache-spark-lazy-evaluation/), it can be more efficient for working with large datasets. This is ideal for data visualizations that we present in the results section below. We ran the following data exploration:
1. ADD IN HERE

Code for our Data Exploration can be found HERE

## Sentiment Classification Model

Our Sentiment Classifier allowed us to understand some of the emotion behind the way that people comment on content from sex workers. PySpark allows for a distributed computing framework which allows us to perform the Sentiment Analysis tasks much more efficiently for our dataset. The Sentiment Analysis task was implemented in the following steps:
1. STEPS HERE

Code for the sentiment analysis can be found HERE

## Sexism Classifier

We explored the sexism present against female sex workers online by running a Sexism classifier on Pyspark. Pyspark's high computation power was well suited for our model which used text data as input for our classification model. Our Sexism classifier runs the following steps:
1. STEPS HERE

Code for the sexism classification model can be found HERE

### Results

OVERALL STATEMENT HERE

#### Data Exploration

#### Sentiment Analysis

#### Sexism Classification Model

### Discussion

### Conclusion

### References
- Pihlaja, S. (2016). Expressing pleasure and avoiding engagement in online adult video comment sections. Journal of Language and Sexuality, 5(1), 94–112. [https://doi.org/10.1075/jls.5.1.04pih](https://doi.org/10.1075/jls.5.1.04pih) 
- 