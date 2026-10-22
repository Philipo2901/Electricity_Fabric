# Data Lineage – Silver to Gold (Sales)

Generated: 2026-02-20 15:40:17 UTC

## Übersicht

| Notebook | Silver Sources | Gold Target |
|---|---|---|
| [03a_NB_MergeIntoDimCustomer](fabric/03_Silver2Gold/Sales/03a_NB_MergeIntoDimCustomer.Notebook/notebook-content.py) | LH_Silver.AdventureWorks.SalesLTCustomer<br>LH_Silver.AdventureWorks.SalesLTCustomerAddress<br>LH_Silver.AdventureWorks.SalesLTAddress | LH_Gold.Sales.DimCustomer |
| [03b_NB_MergeIntoDimProduct](fabric/03_Silver2Gold/Sales/03b_NB_MergeIntoDimProduct.Notebook/notebook-content.py) | LH_Silver.AdventureWorks.SalesLTProduct<br>LH_Silver.AdventureWorks.SalesLTProductCategory<br>LH_Silver.AdventureWorks.SalesLTProductDescription<br>LH_Silver.AdventureWorks.SalesLTProductModel<br>LH_Silver.AdventureWorks.SalesLTProductModelProductDescription | LH_Gold.Sales.DimProduct |
| [03c_NB_MergeIntoFactSales](fabric/03_Silver2Gold/Sales/03c_NB_MergeIntoFactSales.Notebook/notebook-content.py) | LH_Silver.AdventureWorks.SalesLTSalesOrderHeader<br>LH_Silver.AdventureWorks.SalesLTSalesOrderDetail | LH_Gold.Sales.FactSales |

## Lineage Graph

::: mermaid
flowchart LR
    LH_Gold_Sales_DimCustomer["LH_Gold.Sales.DimCustomer"]
    LH_Silver_AdventureWorks_SalesLTCustomer["LH_Silver.AdventureWorks.SalesLTCustomer"]
    LH_Silver_AdventureWorks_SalesLTCustomer --> LH_Gold_Sales_DimCustomer
    LH_Silver_AdventureWorks_SalesLTCustomerAddress["LH_Silver.AdventureWorks.SalesLTCustomerAddress"]
    LH_Silver_AdventureWorks_SalesLTCustomerAddress --> LH_Gold_Sales_DimCustomer
    LH_Silver_AdventureWorks_SalesLTAddress["LH_Silver.AdventureWorks.SalesLTAddress"]
    LH_Silver_AdventureWorks_SalesLTAddress --> LH_Gold_Sales_DimCustomer
    LH_Gold_Sales_DimProduct["LH_Gold.Sales.DimProduct"]
    LH_Silver_AdventureWorks_SalesLTProduct["LH_Silver.AdventureWorks.SalesLTProduct"]
    LH_Silver_AdventureWorks_SalesLTProduct --> LH_Gold_Sales_DimProduct
    LH_Silver_AdventureWorks_SalesLTProductCategory["LH_Silver.AdventureWorks.SalesLTProductCategory"]
    LH_Silver_AdventureWorks_SalesLTProductCategory --> LH_Gold_Sales_DimProduct
    LH_Silver_AdventureWorks_SalesLTProductDescription["LH_Silver.AdventureWorks.SalesLTProductDescription"]
    LH_Silver_AdventureWorks_SalesLTProductDescription --> LH_Gold_Sales_DimProduct
    LH_Silver_AdventureWorks_SalesLTProductModel["LH_Silver.AdventureWorks.SalesLTProductModel"]
    LH_Silver_AdventureWorks_SalesLTProductModel --> LH_Gold_Sales_DimProduct
    LH_Silver_AdventureWorks_SalesLTProductModelProductDescription["LH_Silver.AdventureWorks.SalesLTProductModelProductDescription"]
    LH_Silver_AdventureWorks_SalesLTProductModelProductDescription --> LH_Gold_Sales_DimProduct
    LH_Gold_Sales_FactSales["LH_Gold.Sales.FactSales"]
    LH_Silver_AdventureWorks_SalesLTSalesOrderHeader["LH_Silver.AdventureWorks.SalesLTSalesOrderHeader"]
    LH_Silver_AdventureWorks_SalesLTSalesOrderHeader --> LH_Gold_Sales_FactSales
    LH_Silver_AdventureWorks_SalesLTSalesOrderDetail["LH_Silver.AdventureWorks.SalesLTSalesOrderDetail"]
    LH_Silver_AdventureWorks_SalesLTSalesOrderDetail --> LH_Gold_Sales_FactSales
:::

## Column-Level Lineage

Die folgenden Mappings werden statisch aus den `df_transformed.select(...)` Ausdrücken der Notebooks abgeleitet.

### LH_Gold.Sales.DimCustomer

Notebook: [03a_NB_MergeIntoDimCustomer](fabric/03_Silver2Gold/Sales/03a_NB_MergeIntoDimCustomer.Notebook/notebook-content.py)

| Target Column | Source Column(s) | Source Table(s) | Expression |
|---|---|---|---|
| CustomerSK | (keine direkte Quellspalte erkannt) | (n/a) | F.lit(max_identity_column + F.monotonically_increasing_id()+1).alias(SURROGATE_KEY) |
| CustomerID | df_customer.CustomerID | LH_Silver.AdventureWorks.SalesLTCustomer | df_customer["CustomerID"] |
| Title | df_customer.Title | LH_Silver.AdventureWorks.SalesLTCustomer | df_customer["Title"] |
| FirstName | df_customer.FirstName | LH_Silver.AdventureWorks.SalesLTCustomer | df_customer["FirstName"] |
| LastName | df_customer.LastName | LH_Silver.AdventureWorks.SalesLTCustomer | df_customer["LastName"] |
| CompanyName | df_customer.CompanyName | LH_Silver.AdventureWorks.SalesLTCustomer | df_customer["CompanyName"] |
| AddressType | df_customer_address.AddressType | LH_Silver.AdventureWorks.SalesLTCustomerAddress | df_customer_address["AddressType"] |
| AddressLine1 | df_address.AddressLine1 | LH_Silver.AdventureWorks.SalesLTAddress | df_address["AddressLine1"] |
| AddressLine2 | df_address.AddressLine2 | LH_Silver.AdventureWorks.SalesLTAddress | df_address["AddressLine2"] |
| City | df_address.City | LH_Silver.AdventureWorks.SalesLTAddress | df_address["City"] |
| CountryRegion | df_address.CountryRegion | LH_Silver.AdventureWorks.SalesLTAddress | df_address["CountryRegion"] |
| PostalCode | df_address.PostalCode | LH_Silver.AdventureWorks.SalesLTAddress | df_address["PostalCode"] |

### LH_Gold.Sales.DimProduct

Notebook: [03b_NB_MergeIntoDimProduct](fabric/03_Silver2Gold/Sales/03b_NB_MergeIntoDimProduct.Notebook/notebook-content.py)

| Target Column | Source Column(s) | Source Table(s) | Expression |
|---|---|---|---|
| ProductSK | (keine direkte Quellspalte erkannt) | (n/a) | F.lit(max_identity_column + F.monotonically_increasing_id()+1).alias(SURROGATE_KEY) |
| ProductID | p.ProductID | LH_Silver.AdventureWorks.SalesLTProduct | "p.ProductID" |
| Name | p.Name | LH_Silver.AdventureWorks.SalesLTProduct | "p.Name" |
| Description | pd.Description | LH_Silver.AdventureWorks.SalesLTProductDescription | "pd.Description" |
| ProductNumber | p.ProductNumber | LH_Silver.AdventureWorks.SalesLTProduct | "p.ProductNumber" |
| ProductCategory | pc.Name | LH_Silver.AdventureWorks.SalesLTProductCategory | F.col("pc.Name").alias("ProductCategory") |
| ParentProductCategory | ppc.Name | LH_Silver.AdventureWorks.SalesLTProductCategory | F.col("ppc.Name").alias("ParentProductCategory") |
| ProductModel | pm.Name | LH_Silver.AdventureWorks.SalesLTProductModel | F.col("pm.Name").alias("ProductModel") |
| Color | p.Color | LH_Silver.AdventureWorks.SalesLTProduct | "p.Color" |
| ListPrice | p.ListPrice | LH_Silver.AdventureWorks.SalesLTProduct | "p.ListPrice" |
| Size | p.Size | LH_Silver.AdventureWorks.SalesLTProduct | "p.Size" |
| Weight | p.Weight | LH_Silver.AdventureWorks.SalesLTProduct | "p.Weight" |
| SellStartDate | p.SellStartDate | LH_Silver.AdventureWorks.SalesLTProduct | "p.SellStartDate" |
| SellEndDate | p.SellEndDate | LH_Silver.AdventureWorks.SalesLTProduct | "p.SellEndDate" |

### LH_Gold.Sales.FactSales

Notebook: [03c_NB_MergeIntoFactSales](fabric/03_Silver2Gold/Sales/03c_NB_MergeIntoFactSales.Notebook/notebook-content.py)

| Target Column | Source Column(s) | Source Table(s) | Expression |
|---|---|---|---|
| SalesSK | (keine direkte Quellspalte erkannt) | (n/a) | F.lit(max_identity_column + F.monotonically_increasing_id()+1).alias(SURROGATE_KEY) |
| SalesOrderDetailID | sod.SalesOrderDetailID | LH_Silver.AdventureWorks.SalesLTSalesOrderDetail | "sod.SalesOrderDetailID" |
| CustomerSK | dc.CustomerSK | LH_Gold.Sales.DimCustomer | "dc.CustomerSK" |
| ProductSK | dp.ProductSK | LH_Gold.Sales.DimProduct | "dp.ProductSK" |
| OrderDate | soh.OrderDate | LH_Silver.AdventureWorks.SalesLTSalesOrderHeader | "soh.OrderDate" |
| DueDate | soh.DueDate | LH_Silver.AdventureWorks.SalesLTSalesOrderHeader | "soh.DueDate" |
| ShipDate | soh.ShipDate | LH_Silver.AdventureWorks.SalesLTSalesOrderHeader | "soh.ShipDate" |
| IsOnlineOrder | soh.OnlineOrderFlag | LH_Silver.AdventureWorks.SalesLTSalesOrderHeader | F.col("soh.OnlineOrderFlag").alias("IsOnlineOrder") |
| OrderQuantity | sod.OrderQty | LH_Silver.AdventureWorks.SalesLTSalesOrderDetail | F.col("sod.OrderQty").alias("OrderQuantity") |
| UnitPrice | sod.UnitPrice | LH_Silver.AdventureWorks.SalesLTSalesOrderDetail | "sod.UnitPrice" |
| UnitPriceDiscount | sod.UnitPriceDiscount | LH_Silver.AdventureWorks.SalesLTSalesOrderDetail | "sod.UnitPriceDiscount" |
| LineTotal | sod.LineTotal | LH_Silver.AdventureWorks.SalesLTSalesOrderDetail | "sod.LineTotal" |
