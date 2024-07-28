{{
    config(
        materialized = "table",
        file_format = "parquet",
        location_root = "/mnt/gold/customers"
    )
}}

with address_data as (
    select
        AddressID,
        AddressLine1,
        AddressLine2,
        City,
        StateProvince,
        CountryRegion,
        PostalCode
    from {{ ref('address_data') }} where dbt_valid_to is null
)

, customer_address_data as (
    select
        CustomerId,
        AddressId,
        AddressType
    from {{ref('customer_address_data')}} where dbt_valid_to is null
)

, customer_info as (
    select
        CustomerId,
        concat(ifnull(FirstName,' '),' ',ifnull(MiddleName,' '),' ',ifnull(LastName,' ')) as FullName
    from {{ref('customer_info')}} where dbt_valid_to is null
)

, transformed_data as (
    select
    row_number() over (order by customer_info.customerid) as customer_sk, -- auto-incremental surrogate key
    customer_info.CustomerId,
    customer_info.fullname,
    customer_address_data.AddressID,
    customer_address_data.AddressType,
    address_data.AddressLine1,
    address_data.City,
    address_data.StateProvince,
    address_data.CountryRegion,
    address_data.PostalCode
    from customer_info
    inner join customer_address_data on customer_info.CustomerId = customer_address_data.CustomerId
    inner join address_data on customer_address_data.AddressID = address_data.AddressID
)
select *
from transformed_data
