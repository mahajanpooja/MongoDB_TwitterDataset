# MongoDB_TwitterDataset
This project demonstrates practical MongoDB queries to explore real world data using indexes which help to improve execution performance.
The two data sets consist of tweet and user objects downloaded from Twitter using Twitter API.

Query workload:

[Q1] Find the number of general tweets with at least one reply and one retweet in the
data set. Note that a general tweet is a tweet with neither a replyto_id field, nor a
retweet_id field; a reply is a tweet with the replyto_id field; a retweet is a tweet
with the retweet_id field.

[Q2] Find the reply tweet that has the most retweets in the data set.

[Q3] Find the top 5 hashtags appearing as the FIRST hashtag in a general or reply
tweet, ignoring the case of the hashtag. Note that the order does not matter if a few
hashtags have the same occurrence number.

[Q4] For a given hash_tag, there are many tweets including that hash_tag. Some
of those tweets mention one or many users. Among all users mentioned in those
tweets, find the top 5 users with the most followers_count. For each user, you
should print out the id, name and location. Not all users have a profile in the users
data set; you can ignore those that do not have a profile. If there are less than 5 users
with profile, print just those users with a profile.

[Q5] Find the number of general tweets published by users with neither location nor
description information.

[Q6] Find the general tweet that receives most retweets in the first hour after it is
published. Print out the tweet Id and the number of retweets it received within the
first hour.
