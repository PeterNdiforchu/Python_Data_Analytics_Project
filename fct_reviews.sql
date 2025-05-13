{{
  config(
    materialized = 'incremental',
    on_schema_change='sync_all_columns'
    )
}}
WITH src_reviews AS (
    SELECT 
      listing_id, 
      review_date, 
      reviewer_name, 
      review_text
    FROM {{ ref('src_reviews') }}
)
SELECT *
FROM src_reviews
WHERE review_text is not null
{% if is_incremental() %}
  and review_date > (select max(review_date) from {{ this }})
{% endif %}