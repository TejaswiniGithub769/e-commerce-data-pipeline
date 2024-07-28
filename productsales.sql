{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/gold/products"
    )
}}

with prod_snapshot as (
    select
        prod_id,
        prod_name,
        prod_cost,
        prod_price,
        prod_size,
        prod_weight,
        prod_category_id,
        prod_model_id,
        sell_start_date,
        sell_end_date,
        disc_date
    from {{ ref("prod_snapshot") }}
    where dbt_valid_to is null
),

prod_model_snapshot as (
    select
        prod_model_id,
        model_name,
        catalog_desc,
        row_number() over (order by model_name) as model_id
    from {{ ref("prod_model_snapshot") }}
    where dbt_valid_to is null
),

transformed as (
    select
        row_number() over (order by p.prod_id) as product_sk,
        p.prod_name,
        p.prod_cost,
        p.prod_price,
        p.prod_size,
        p.prod_weight,
        pm.model_name as model,
        pm.catalog_desc as description,
        p.sell_start_date,
        p.sell_end_date,
        p.disc_date
    from prod_snapshot p
    left join prod_model_snapshot pm on p.prod_model_id = pm.prod_model_id
)

select * from transformed





//Sales sql code



{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/gold/sales_data"
    )
}}

with sales_detail_snapshot as (
    SELECT
        SalesOrderID,
        SalesOrderDetailID,
        OrderQty,
        ProductID,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal
    FROM {{ ref("sales_detail_snapshot") }}
),

product_data_snapshot as (
    SELECT
        ProductID,
        Name,
        ProductNumber,
        Color,
        StandardCost,
        ListPrice,
        Size,
        Weight,
        SellStartDate,
        SellEndDate,
        DiscontinuedDate,
        ThumbNailPhoto,
        ThumbnailPhotoFileName
    FROM {{ source('sales_data', 'product') }}
),

sales_header_snapshot as (
    SELECT
        SalesOrderID,
        RevisionNumber,
        OrderDate,
        DueDate,
        ShipDate,
        Status,
        OnlineOrderFlag,
        SalesOrderNumber,
        PurchaseOrderNumber,
        AccountNumber,
        CustomerID,
        ShipToAddressID,
        BillToAddressID,
        ShipMethod,
        CreditCardApprovalCode,
        SubTotal,
        TaxAmt,
        Freight,
        TotalDue,
        Comment,
        row_number() over (partition by SalesOrderID order by SalesOrderID) as row_num
    FROM {{ source('sales_data', 'sales_order_header') }}
),

transformed_sales as (
    select
        sd.SalesOrderID,
        sd.SalesOrderDetailID,
        sd.OrderQty,
        sd.ProductID,
        sd.UnitPrice,
        sd.UnitPriceDiscount,
        sd.LineTotal,
        pd.Name,
        pd.ProductNumber,
        pd.Color,
        pd.StandardCost,
        pd.ListPrice,
        pd.Size,
        pd.Weight,
        pd.SellStartDate,
        pd.SellEndDate,
        pd.DiscontinuedDate,
        pd.ThumbNailPhoto,
        pd.ThumbnailPhotoFileName,
        sh.RevisionNumber,
        sh.OrderDate,
        sh.DueDate,
        sh.ShipDate,
        sh.Status,
        sh.OnlineOrderFlag,
        sh.SalesOrderNumber,
        sh.PurchaseOrderNumber,
        sh.AccountNumber,
        sh.CustomerID,
        sh.ShipToAddressID,
        sh.BillToAddressID,
        sh.ShipMethod,
        sh.CreditCardApprovalCode,
        sh.SubTotal,
        sh.TaxAmt,
        sh.Freight,
        sh.TotalDue,
        sh.Comment
    from sales_detail_snapshot sd
    left join product_data_snapshot pd on sd.ProductID = pd.ProductID
    left join sales_header_snapshot sh on sd.SalesOrderID = sh.SalesOrderID
)

select * from transformed_sales
