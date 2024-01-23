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

CREATE OR REPLACE TABLE sparse_features.coffee_reviews AS (
   SELECT ROW_NUMBER() OVER() AS review_number, text, REGEXP_EXTRACT_ALL(LOWER(text), '[a-z]{2,}') AS words, stars
   FROM (SELECT text, cb.stars
         FROM yelp_dataset.review AS r
         INNER JOIN yelp_datset.Coffee_businesses AS cb ON cb.business_id = r.business_id
         WHERE cb.stars = 5)
);





