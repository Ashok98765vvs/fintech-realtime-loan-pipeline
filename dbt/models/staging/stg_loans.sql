-- =============================================================================
-- stg_loans.sql
-- Staging model: Clean and standardize raw loan events from Snowflake raw layer
-- Materialization: view (refreshed on every dbt run)
-- Author: Ashok Chowdary
-- =============================================================================

{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'loans', 'daily']
) }}

WITH source AS (

    SELECT * FROM {{ source('raw', 'loan_events') }}

),

cleaned AS (

    SELECT
        -- Primary Keys
        event_id                                            AS event_id,
        loan_id                                             AS loan_id,
        borrower_id                                         AS borrower_id,
        tenant_id                                           AS tenant_id,

        -- Event Classification
        UPPER(TRIM(event_type))                             AS event_type,
        UPPER(TRIM(loan_type))                              AS loan_type,
        UPPER(TRIM(loan_purpose))                           AS loan_purpose,

        -- Dates
        TRY_CAST(origination_date AS DATE)                  AS origination_date,
        TRY_CAST(maturity_date AS DATE)                     AS maturity_date,
        DATEDIFF('month', origination_date, maturity_date)  AS loan_term_months,
        DATEDIFF('year', origination_date, CURRENT_DATE())  AS loan_age_years,

        -- Financial Amounts
        ROUND(original_balance, 2)                          AS original_balance,
        ROUND(current_balance, 2)                           AS current_balance,
        ROUND(original_balance - current_balance, 2)        AS principal_paid,
        ROUND(
            (original_balance - current_balance) / NULLIF(original_balance, 0),
            4
        )                                                   AS pct_principal_paid,
        ROUND(interest_rate, 6)                             AS interest_rate,
        ROUND(interest_rate * 100, 4)                       AS interest_rate_pct,
        ROUND(monthly_payment, 2)                           AS monthly_payment,
        ROUND(payment_amount, 2)                            AS payment_amount,

        -- Delinquency
        COALESCE(days_past_due, 0)                          AS days_past_due,
        CASE
            WHEN days_past_due = 0     THEN 'CURRENT'
            WHEN days_past_due <= 30   THEN '30_DPD'
            WHEN days_past_due <= 60   THEN '60_DPD'
            WHEN days_past_due <= 90   THEN '90_DPD'
            WHEN days_past_due <= 120  THEN '120_DPD'
            ELSE 'SEVERE_DELINQUENCY'
        END                                                 AS delinquency_bucket,
        days_past_due > 90                                  AS is_seriously_delinquent,

        -- Borrower Profile
        property_state                                      AS property_state,
        credit_score                                        AS credit_score,
        CASE
            WHEN credit_score >= 750 THEN 'EXCELLENT'
            WHEN credit_score >= 700 THEN 'GOOD'
            WHEN credit_score >= 650 THEN 'FAIR'
            WHEN credit_score >= 600 THEN 'POOR'
            ELSE 'VERY_POOR'
        END                                                 AS credit_tier,
        ROUND(ltv_ratio, 4)                                 AS ltv_ratio,
        ROUND(ltv_ratio * 100, 2)                           AS ltv_pct,
        ltv_ratio > 0.80                                    AS is_high_ltv,
        ROUND(dti_ratio, 4)                                 AS dti_ratio,
        ROUND(dti_ratio * 100, 2)                           AS dti_pct,
        dti_ratio > 0.43                                    AS is_high_dti,

        -- Data Quality Flags
        is_valid                                            AS is_valid,
        CASE
            WHEN event_id IS NULL        THEN FALSE
            WHEN loan_id IS NULL         THEN FALSE
            WHEN borrower_id IS NULL     THEN FALSE
            WHEN original_balance <= 0   THEN FALSE
            WHEN interest_rate <= 0      THEN FALSE
            WHEN origination_date IS NULL THEN FALSE
            ELSE TRUE
        END                                                 AS is_complete_record,

        -- Timestamps
        event_timestamp                                     AS event_timestamp,
        CAST(event_timestamp AS DATE)                       AS event_date,
        DATE_TRUNC('month', event_timestamp)                AS event_month,
        loaded_at                                           AS loaded_at

    FROM source
    WHERE is_valid = TRUE

)

SELECT * FROM cleaned
