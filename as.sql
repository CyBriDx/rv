# Databricks notebook source
 %md
 #####importing utilities required for notebook execution


 %run "/Shared/SSA D&A/Utilities"


dbutils.widgets.text('params', '', '')
dimTime_path = generate_filePath(adlsAccountName, 'external/AMESA/SSA/POS/Dim_Time')
pnpDeliveries_path = generate_filePath(adlsAccountName, 'external/AMESA/SSA/POS/PnPDeliveries/')
PnPSales_Path = generate_filePath(adlsAccountName, "external/AMESA/SSA/PnPSales/")
TEGMDSTOREMASTER_Path = generate_filePath(adlsAccountName, "external/AMESA/SSA/TEGMDSTOREMASTER/")
PnPStoreValidaty_Path = generate_filePath(adlsAccountNamasde, "external/AMESA/SSA/PnPStoreValidaty/")

sourceFiletype = "delta"
inferSchema = True
firstRowIsHeader = True
delimiter = ","

# ### Test script to check values assigned to variables
# print(materialMaster_path)


materialMaster_df = get_Table_From_Synapse("dbo.MaterialMaster")
storeMaster_df = get_Table_From_Synapse("dbo.StoreMaster")


pnpDeliveries_df = get_data_from_blob_adls_using_FilePath(pnpDeliveries_path, sourceFiletype, inferSchema, firstRowIsHeader, delimiter)
pnpDeliveries_df.createOrReplaceTempView("pnpDeliveries")

storeMaster_df.createOrReplaceTempView("storeMaster")

materialMaster_df.createOrReplaceTempView("materialMaster")

DimTime_df = get_data_from_blob_adls_using_FilePath(dimTime_path, sourceFiletype, inferSchema, firstRowIsHeader, delimiter)
DimTime_df.createOrReplaceTempView("DimTime")



 %sql
 DROP VIEW IF EXISTS vw_PnPDeliveries;
 CREATE temporary VIEW vw_PnPDeliveries
 as (
       WITH CTE_StoreMaster
       AS
       (
         SELECT DISTINCT StoreInternalCod
               ,PnPStore
               ,PnPRegion
               ,PFIRegion
               ,PnPMajorRegion
               ,PnPDemo
               ,PnPStoreRangeSize
               ,StatusOfStore
               ,StoreName
               ,StoreType
               ,BuyingGroup
               ,BuyingGroupRegion
               ,PFIAS400Number
               ,ManualPFIRSM
               ,`ManualPFIFieldManager/AreaMarketer`
               ,SupplyingBakery
               ,FMCALLDAY
               ,FMFREQUANCY
               ,InstoreMerchandiseHoursAllInstoreStaff
               -- ,AztecAlignment
               ,OpeningDay
               ,ClosingDay
               ,NewDemographic
               ,PnPDCSupport
               ,DateUpdated
               ,ExecutracStore
               ,StoreValidFrom
               ,StoreValidTo
               ,BakeryDeliveryFrequancy
               ,PromoSupport
         FROM storeMaster
         WHERE StoreInternalCod IS NOT NULL
       )
       ,CTE_MaterMaster
       AS
       (
         SELECT DISTINCT PnPArticleNumber
                ,PnPArticle
                ,PnPCategory
                ,PnPVendor
                ,PnPBrand
                ,CompanyCode
                ,ShopriteArticleNumber
                ,SKUStrategicStatus
                ,MaterialStatus
                ,CASBarCode
                ,ICBarCode
                ,PCBarCode
                ,OtherSizeBarCode
                ,ExceptionSizeBarCode
                ,ShopriteCASArticleNumber
                ,ShopriteICArticleNumber
                ,Date_Updated
                ,ExecutracMaterial
                ,MATVAlidFrom
                ,MATVAlidTo
                ,VATCode
                ,PackSize
                ,PackConfiguration
                ,Brand
                ,SAP_Material_Key
                ,ProdDivision
                ,ProdCategory
                ,ProdSubCategory
                ,ProdAdditionalCategory
                ,MaterialKey
                ,Material
                ,OtherFactorToCU
                ,ExceptionFactorToCU
                ,CASFactorToCU
                ,ICFactorToCU
         FROM materialMaster
         WHERE PnPArticleNumber IS NOT NULL
       )
       ,CTE_1
       (
         SELECT del.*,
         CASE 
           WHEN date_format(del.receivingAdvice_creationDateTime, 'yyyy-MM-dd')<'2023-04-26' 
             THEN '2023-04-14'
           ELSE '2023-04-27' 
         END AS JoinKey FROM pnpDeliveries del
       )
       ,CTE_PnPDeliveries_Transformations
       (
         SELECT date_format(receivingAdvice_creationDateTime, 'yyyyMM') AS `CalendarYearMonth`
             , MONTH(receivingAdvice_creationDateTime) AS `CalendarMonth`
             , YEAR(receivingAdvice_creationDateTime) AS `CalendarYear`
             , MM.Brand AS `ProdBrand`
             , MM.PnPBrand AS `Brand_Transaction`
             , SM.BuyingGroup AS `BuyingGroup`
             , MM.CompanyCode AS `Company`
             ,CASE 
                  WHEN MM.CompanyCode = 'SASKO GRAIN'
                    THEN MM.CompanyCode
                  WHEN MM.CompanyCode = 'Sasko Baking'
                    THEN MM.CompanyCode
                  ELSE 'Unmapped Categories'
              END AS `Company_T`
             , Del.receivingAdvice_creationDateTime AS `Date`
             , MM.ExecutracMaterial AS `ProdExecutracStatus`
             , SM.ExecutracStore AS `StoreExecutracStatus`
             , MM.Material AS `Material`
             , MM.MaterialStatus AS `ProdStatus`
             , MM.PnPArticle AS `PnPArticle`
             , MM.PnPBrand AS `PnPBrand`
             , MM.PnPCategory AS `PnPCategory`
             , SM.PnPDCSupport AS `DCSupport`
             , concat(date_format(del.receivingAdvice_creationDateTime, 'yyyyMMdd'), del.shipToapiv, del.BuyerArticleCode) AS `Key`
             , SM.PnPMajorRegion AS `PnPMajorRegion`
             , Del.number AS `PnPMovementItem`
             , Del.InstanceIdentifier AS `PnPMovementNumber`
             , Del.receivingAdviceNr AS `PnPMovementSubNumber`
             , del.TransactionType AS `PnPMovementTransaction`
             , del.PO_Number AS `PnPPurchaseDocument`
             , SM.PnPRegion AS `PnPRegion`
             , SM.PFIRegion AS `PFIRegion`
             , del.BuyerArticleCode AS `PnPArticleNumber`
             , SM.PnPStore AS `PnPStore`
             , del.shipToapiv AS `PnPStoreID`
             , SM.StoreValidFrom 
             , SM.StoreValidTo
             , MM.MATVAlidFrom
             , MM.MATVAlidTo
             , MM.ProdCategory AS `ProdCategory`
             , MM.ProdDivision AS `ProdDivision`
             , MM.ProdSubCategory AS `ProdSubCategory`
             , MM.SKUStrategicStatus AS `ProdStratetyStatus`
             , SM.StatusOfStore AS `StoreStatus`
             , SM.StoreName AS `Store`
             , SM.StoreType AS `StoreType`
             , SM.StoreType AS `StoreType_Transaction`
             , SM.SupplyingBakery AS `Bakery`
              ,CASE 
                  WHEN MM.CompanyCode = 'SASKO GRAIN'
                    THEN 'Sasko GRAIN'
                  WHEN MM.CompanyCode = 'Sasko Baking'
                    THEN SM.SupplyingBakery
                  ELSE ''
                END AS `Bakery_T`
              ,CASE 
                  WHEN TransactionType = 'GoodsReceipt'
                    THEN QuantityReceived
                  ELSE 0
                END AS `PnPGoodsReceipts`
             ,QuantityReceived AS `PnPGoodsMovement`
             
             , CASE
                 WHEN UOM_Received = 'EA'
                   THEN coalesce(QuantityReceived,0) * coalesce(ICFactorToCU,0)
                 ELSE coalesce(QuantityReceived,0) * coalesce(CASFactorToCU,0)
                END AS `PNPMMQtyCU`
                
             , 1 AS `PnPMovementRecords`
             , CASE 
                  WHEN TransactionType = 'GoodsReceipt'
                    THEN QuantityReceived
                  ELSE 0
                END AS `PnPGoodsReturned`
              , 0 AS `PnPGRVRecords`
              ,Del.Silver_LoadTime
              ,FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'IST') AS Gold_LoadTime
         FROM CTE_1 AS Del
         LEFT JOIN CTE_StoreMaster AS SM
           ON CONCAT(Del.shipToapiv,Del.JoinKey) = CONCAT(SM.StoreInternalCod,SM.DateUpdated)
         LEFT JOIN CTE_MaterMaster AS MM
           ON CONCAT(Del.BuyerArticleCode,Del.JoinKey)  = CONCAT(MM.PnPArticleNumber,MM.Date_Updated)
         WHERE receivingAdvice_creationDateTime IS NOT NULL
       )
       SELECT *
             , CASE 
                 WHEN `PnPMovementTransaction` = 'GoodsReceipt'
                   THEN `PNPMMQtyCU`
                 ELSE 0
               END AS `PnPQtyReceivedCU`
             , CASE 
                 WHEN `PnPMovementTransaction` = 'GoodsReturn'
                   THEN `PNPMMQtyCU`
                 ELSE 0
               END AS `PnPQtyReturnedCU`
       FROM CTE_PnPDeliveries_Transformations
 )


df_PnPDeliveries = sqlContext.sql("SELECT * FROM vw_PnPDeliveries")
append_Overwrite_Table_In_Synapse(df_PnPDeliveries, "dbo.PnPDeliveries", "overwrite")



