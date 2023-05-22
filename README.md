# Tracking Sexism in Pornhub Comments
## Pornhub Scholars: Kaya Borlase, Joseph Helbing, Loizos Bitsikokos
## MACS 30123
## Winter 2023

### Introduction

In the digital age, there is a tremendous surplus of sexual content online, while visibility of sex works (and their rights) is becoming increasingly relevant. The amount of online activity around sexual content has the potential to have real-world, particularly harmful implications, as the lives of sex workers could be exploited or endangered. Online pornographic content has also severe implications for how content consumers perceive women (note that the term women in this context is used to refer to women regardless of their sex assigned at birth). A brief literature search we conducted suggests that sexual online content and the public discourse around it in platforms offering pornographic content is severely understudied, while the only computationally significant studies of such content are conducted by the platforms themselves. This project explores the perception of female sex workers online by conducting a content analysis on comments scraped from Pornhub, a pornographic video-streaming website (see this [Wiki Article](https://en.wikipedia.org/wiki/Pornhub) for more information). We built a large-scale web-scraping and data pipeline on AWS that scraped comments from videos. We additionally conducted a data exploration and sentiment analysis to gain a general perception of the comment space. Finally, we trained sexism detector modelings to evaluate the amount of sexism prevalent in comments towards female sex workers online. Overall, our research answers the following research questions: 

1. In what ways do people interact with sex workers online?
2. To what extent is there sexism in these interactions? Are they positive? Negative? Or even nasty?

### Background and Significance

ADD BACKGROUND AND SIGNIFICANCE AND JUSTIFY WHY WE USED LARGE SCALE COMPUTING FOR THIS PROBLEM!!

### Data and Methods

As previously stated, we scraped the top comments under videos posted to the online pornography site, PornHub. We gathered a total of ENTER_NUMBER_HERE comments. After cleaning the comments by removing stop words, we proceeded to the analysis. Overall, because we have a fairly large dataset of text data, it made sense to use scalable computing at each phase of the project. An explanation and justification of each method is presented below.

#### Data Collection

To collect the data, we implemented a parallelized scraper using a lambda function via AWS. Because we wanted to scrape comments from a large number of videos, we wanted to be able to visit multiple pages at once to speed up the process. Additionally, our lambda function wrote the comments directly into an [Amazon RDS](https://aws.amazon.com/rds/) so we did not have to worry about a memory bottleneck if we transfered the data back to our local machine. Overall, our scraper ran the following steps:
1. JOE ADD SIMPLE STEPS HERE

This scraper allowed us to scrape and store a large number of comments that we likely wouldn't have been able to scrape in a reasonable time frame on our local machines. We recognize that the more comments we have, the more likely it is that we have a dataset that accurately represents the comments that product consumer we will be able to get a dataset that represents the way that content consumers of pornographic material are reacting to female sex workers online.

Code for the Scraper can be found HERE

#### Data Exploration

In order to explore the data, we use Pyspark to visualize different aspects of the text data. Because Pyspark uses a [lazy evaluation model](https://data-flair.training/blogs/apache-spark-lazy-evaluation/), it can be more efficient for working with large datasets. This is ideal for data visualizations that we present in the results section below. We ran the following data exploration:
1. ADD IN HERE

Code for our Data Exploration can be found HERE

#### Sentiment Classification Model

Our Sentiment Classifier allowed us to understand some of the emotion behind the way that people comment on content from sex workers. PySpark allows for a distributed computing framework which allows us to perform the Sentiment Analysis tasks much more efficiently for our dataset. The Sentiment Analysis task was implemented in the following steps:
1. STEPS HERE

Code for the sentiment analysis can be found HERE

#### Sexism Classifier

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

