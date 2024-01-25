/*
Yelp Reviews Data Analysis
Skills used: Aggregate Functions, Joins, Window Functions, Subqueries/CTE's, Data Types, Creating tables/views, Modeling
/*


-- Cleaning & Removing Unnecessary Data
-- Inadverent state location

     DELETE FROM yelp_dataset.Business
     WHERE state = 'XMS';

--Changing Nulls to zero

    UPDATE yelp_dataset.User
    SET elite = 0
    WHERE elite IS NOT NULL

--Remove Unnecessary Columns

    ALTER TABLE yelp_dataset.Business
    DROP COLUMN longitude,
    DROP COLUMN latitude

--Determine Range of Data

    SELECT  MIN(date), MAX(date)
    FROM yelp_datset.review


--Summary Statistics

     SELECT ROUND(AVG(review_count),2) AS avg_user_reviews, MIN(review_count) AS min_user_reviews, MAX(review_count) AS max_user_reviews
     FROM User;

--Seperate into Quartiles
-- Users with less reviews tend to be more negative

     SELECT subq.quartiles, ROUND(AVG(review_count),0) AS average_reviews, ROUND(AVG(subq.average_stars),2) AS average_stars
     FROM 
      (SELECT average_stars, review_count, NTILE(4) OVER(ORDER BY review_count) AS quartiles
      FROM yelp_dataset.user) subq
      GROUP BY quartiles
      ORDER BY quartiles

--Count of Businesses by Star Rating
-- Count increases with star rating other than 5.0

     SELECT stars, COUNT(*) AS business_count
     FROM yelp_dataset.Business
     GROUP BY stars
     ORDER BY stars DESC

--Created subset of Business Table with just Businesses with Coffee as their category using Excel Filtering
--Uploaded excel file back to BigQuery as new Table
-- 649 Coffee Shops in total

     SELECT COUNT(*) 
     FROM yelp_dataset.coffee_businesses

-- Where are they located?

     SELECT city, state, COUNT(*) AS num_shops
     FROM yelp_datset.coffee_businesses

--Correlation between review count and star rating?
--r2 = -0.07

     SELECT corr(review_count, stars)
     FROM yelp_dataset.Coffee_businesses

##Amount of "Check-Ins" per City with Star Ratings of 4.5 or Greater

     WITH location AS(
     SELECT business_id, city, state
     FROM yelp_dataset.Coffee_Businesses
     WHERE stars >= 4.5)
        SELECT city, state, COUNT(c.business_id) AS num_checkins
        FROM location INNER JOIN yelp_dataset.checkin AS c ON location.business_id = c.business_id
     GROUP BY city, state
     ORDER BY num_checkins DESC


--SENTIMENT ANALYSIS on Reviews using Sparse Features
-- Creating new table joining reviews & coffee businesses while extracting text 
-- Create a new dataset to convert text from the reviews into numerics to feed them into machine learning model

     CREATE OR REPLACE TABLE sparse_features.coffee_reviews AS (
         SELECT ROW_NUMBER() OVER() AS review_number, text, REGEXP_EXTRACT_ALL(LOWER(text), '[a-z]{2,}') AS words, stars
         FROM (SELECT text, cb.stars
               FROM yelp_dataset.review AS r
               INNER JOIN yelp_datset.Coffee_businesses AS cb ON cb.business_id = r.business_id
         WHERE cb.stars = 5)
);

-- Run query from review table to split data into test and train randomly

     SELECT split, COUNT(split) AS type
     FROM 
        (SELECT business_id, IF(ABS(MOD(FARM_FINGERPRINT(text),40)) < 20, 'Test', 'Train') AS split
         FROM yelp_datset.review)
     GROUP BY split

-- Selecting created label column made from previous query that classifed reviews as negative if star rating > 3.0 and postive if > 4
-- This binary classification neccessary for model to run
-- Also extracted individual words using REGEXP_EXTRACT_ALL function to build a vocabulary

     CREATE OR REPLACE TABLE sparse_features.coffee_reviews AS
       (SELECT ROW_NUMBER()OVER() AS review_number, text, REGEXP_EXTRACT_ALL(LOWER(text), '[a-z]{2,}') AS words, label, spl
        FROM
             (SELECT DISTINCT text, label, spl
              FROM sparse_features.join_table
              WHERE label IN ('Negative','Positive')
      )
)

-- Creating a vocabulary list

      CREATE OR REPLACE TABLE sparse_features.vocabulary AS (
        SELECT word, word_frequency, word_index
        FROM (
             SELECT word, word_frequency, ROW_NUMBER() OVER(ORDER BY word_frequency DESC) - 1 AS word_index
             FROM(
                  SELECT word, COUNT(word) AS word_frequency
                  FROM sparse_features.coffee_reviews
                  UNNEST(words) AS word
             WHERE spl = 'Train'
             GROUP BY word
            )
          )
         WHERE word_index < 20000 #selecting top 20,000 words based on count
);

-- Creating Model

CREATE OR REPLACE MODEL sparse_features.logisitic_reg_classifier
  TRANSFORM ( * EXCEPT (review_number, review ))
  OPTIONS(MODEL_TYPE='LOGISTIC_REG', INPUT_LABEL_COLS = ['label']) AS
  SELECT review_number, review, spl, label
  FROM sparse_features.sparse_table
  WHERE split = "train";

-- Test Model
-- precision = 0.965 recall = 0.993 accuracy = 0.962 auc = 0.973

    SELECT * FROM ML.EVALUATE(MODEL sparse_features.logistic_reg_classifier,
    (
        SELECT review_number, text, feature, label
        FROM   sparse_features.sparse_table
        WHERE  spl = 'Test'
  )
);

-- Evaluate model for forecasting using common review phrasing
-- creating user reviews to predict if sentiment will be positive or negative

   WITH user_reviews AS (
        SELECT ROW_NUMBER()OVER() AS review_number, text, REGEXP_EXTRACT_ALL(LOWER(text), '[a-z]{2,}') AS words
        FROM (
             SELECT "Fantastic Atmosphere"          AS text UNION ALL
             SELECT "The coffee tastes really good" AS text UNION ALL
             SELECT "Poor Service"                  AS text UNION ALL
             SELECT "Long Wait Times"               AS text 

-- followed by a join query that sparses the forecasted reviews
-- select predicted_label from model 
