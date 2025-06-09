# Databricks notebook source
# MAGIC %sql
# MAGIC select accessibility, address, business_category, business_status, city, country, phone_number, place_information, rating from `dais-hackathon-2025`.nimble.dbx_google_maps_place_daily pd
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `dais-hackathon-2025`.nimble.dbx_google_maps_search_daily where country = 'US'

# COMMAND ----------

# MAGIC %sql
# MAGIC show columns in `dais-hackathon-2025`.nimble.dbx_google_maps_search_daily

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION workspace.acc.google_maps_acc(
# MAGIC     address STRING COMMENT 'Filter by address. Example "51 E Houston St, New York, NY 10012"'
# MAGIC       DEFAULT NULL,
# MAGIC     zipcode STRING COMMENT 'Filter by zipcode. Example: "10012"' DEFAULT NULL,
# MAGIC     city STRING COMMENT 'Filter by city. Example: "New York"' DEFAULT "New York",
# MAGIC     business_category STRING COMMENT 'Filter by business category. Example: "restaurant"'
# MAGIC       DEFAULT NULL
# MAGIC   )
# MAGIC   RETURNS TABLE
# MAGIC   COMMENT 'Returns accessibility data for Google Maps for the city of New York based on the address, zipcode or business_category"'
# MAGIC   RETURN
# MAGIC     with data as (
# MAGIC       select
# MAGIC         title as business_name,
# MAGIC         accessibility['description'] as accessibility_list,
# MAGIC         concat_ws(', ', business_category) AS business_category,
# MAGIC         business_status,
# MAGIC         city,
# MAGIC         country,
# MAGIC         address,
# MAGIC         phone_number,
# MAGIC         place_information,
# MAGIC         rating,
# MAGIC         place_url,
# MAGIC         CAST(regexp_extract(rating, '([0-9]+\\.?[0-9]*)', 1) AS FLOAT) AS rating_numeric
# MAGIC       from
# MAGIC         `dais-hackathon-2025`.nimble.dbx_google_maps_search_daily pd
# MAGIC       where
# MAGIC         -- city ILIKE CONCAT('%New York%')
# MAGIC         -- and 
# MAGIC         country = 'US'
# MAGIC     ),
# MAGIC     flat_list as (
# MAGIC       select
# MAGIC         concat_ws(', ', accessibility_list) AS accessibility,
# MAGIC         *
# MAGIC       from
# MAGIC         data
# MAGIC     )
# MAGIC     select
# MAGIC       *
# MAGIC     from
# MAGIC       flat_list
# MAGIC     WHERE
# MAGIC       1 = 1
# MAGIC       AND accessibility ilike '%Wheelchair%'
# MAGIC       AND (
# MAGIC         isnull(google_maps_acc.address)
# MAGIC         OR google_maps_acc.address = ''
# MAGIC         OR google_maps_acc.address = 'NULL'
# MAGIC         OR address ILIKE CONCAT('%', google_maps_acc.address, '%')
# MAGIC       )
# MAGIC       AND (
# MAGIC         isnull(google_maps_acc.business_category)
# MAGIC         OR google_maps_acc.business_category = ''
# MAGIC         OR google_maps_acc.business_category = 'NULL'
# MAGIC         OR business_category ILIKE CONCAT('%', google_maps_acc.business_category, '%')
# MAGIC       )
# MAGIC       AND (
# MAGIC         isnull(google_maps_acc.city)
# MAGIC         OR google_maps_acc.city = ''
# MAGIC         OR google_maps_acc.city = 'NULL'
# MAGIC         OR city ILIKE CONCAT('%', google_maps_acc.city, '%')
# MAGIC       )
# MAGIC     order by rating_numeric desc limit 50

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM workspace.acc.google_maps_acc(business_category => 'restaurant');