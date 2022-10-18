-- FF MATURITY QUERY 

SELECT t1.f1 as MfgMonthYear, t1.ModelGroup,t1.PCount as PCount,t2.Scount as SCount,t2.MCount as t2.FF1Count, t2.FF2Count,
t2.FF3Count, t2.FF6Count, t2.FF12Count, t2.FF18Count, t2.FF24Count, t2.FF30Count, t2.FF36Count
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF1Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF1Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF3Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF3Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF6Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF6Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF12Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF12Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF18Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF18Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF24Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF24Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF30Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF30Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF36Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF36Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.EOfyc >= 0.95*t2.Scount, "Matured", "NotMatured") AS EOFirstYearMaturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.EOsyc >= 0.95*t2.Scount, "Matured", "NotMatured") AS EOSecondYearMaturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.EOtyc >= 0.95*t2.Scount, "Matured", "NotMatured") AS EOThirdYearMaturity,
FROM
(
  SELECT FORMAT_DATE("%Y-%m", MfgDate) as f1, ModelGroup, count(distinct VehicleChassisNumber) as PCount
  FROM `re-warranty-analytics-prod.re_wa_prod.mfg_mat_table`
  WHERE ModelGroup != 'NA'
  group by f1, ModelGroup
  order by 1
) t1
LEFT JOIN
(
SELECT  FORMAT_DATE("%Y-%m", MfgDate) as f1, 
ModelGroup, 
count(distinct VehicleChassisNumber) as Scount, 
sum(IF(VehicleAge >= 30, 1, 0)) as FF1Count,
sum(IF(VehicleAge >= 90, 1, 0)) as FF3Count,
sum(IF(VehicleAge >= 180, 1, 0)) as FF6Count,
sum(IF(VehicleAge >= 360, 1, 0)) as FF12Count,
sum(IF(VehicleAge >= 540, 1, 0)) as FF18Count,
sum(IF(VehicleAge >= 720, 1, 0)) as FF24Count,
sum(IF(VehicleAge >= 900, 1, 0)) as FF30Count,
sum(IF(VehicleAge >= 1080, 1, 0)) as FF36Count,
sum(IF(VehicleAge >= 365, 1, 0)) as EOfyc,
sum(IF(VehicleAge >= 730, 1, 0)) as EOsyc,
sum(IF(VehicleAge >= 1095, 1, 0)) as EOtyc,

FROM `re-warranty-analytics-prod.re_wa_prod.sales_mat_table`
WHERE ModelGroup != 'NA'
group by f1, ModelGroup
order by 1
) t2
ON t1.f1 = t2.f1 and t1.ModelGroup=t2.ModelGroup
where t1.f1 >= "2019-01" and t1.f1 <= FORMAT_DATE("%Y-%m", CURRENT_DATE()) and t1.ModelGroup is not null
order by t1.f1;




-- 


CREATE TABLE `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v1` AS
SELECT  
a.VehicleChassisNumber, a.JobCard, a.PartNumber, a.PartDescription, a.PartGroup, a.PartVariant, a.PartCategory, 
a.PartDefectCode, a.Zone, a.Region, a.State, a.DealerCode, a.DealerName, a.City, a.Model, a.ModelType, a.Plant, 
a.MfgMonth, a.MfgYear, a.FF1, a.FF3, a.FF6, a.FF12, a.FF18, a.FF24, a.FF30, a.FF36, a.MfgMonthNameYear, a.MfgMonthYear,
 a.DMart, a.PiperrInDate, b.FF1Maturity, b.FF3Maturity, b.FF6Maturity, b.FF12Maturity, b.FF18Maturity, b.FF24Maturity,
  b.FF30Maturity, b.FF36Maturity
FROM `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a
LEFT JOIN `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
ON a.MfgMonthYear = b.MfgMonthYear AND a.ModelType = b.ModelGroup;

#####

create or replace table `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` as
select * from `re-warranty-analytics-prod.re_wa_prod.re_ffff36`


#####
-- 
ENGINE OPENING MATURITY



bq dataset 

-- Part failure dashboard issue

SELECT `MfgMonthNameYear` AS `MfgMonthNameYear`,
       `MfgMonthYear` AS `MfgMonthYear`,
       (COUNT(IF(DMart = 'ff data', JobCard, NULL))) AS `Numerator`,
       (COUNT(DISTINCT IF(DMart = 'dms sales data', VehicleChassisNumber, NULL))+1))*100 AS `Denominator`
       (COUNT(IF(DMart = 'ff data', JobCard, NULL)) / (COUNT(DISTINCT IF(DMart = 'dms sales data', VehicleChassisNumber, NULL))+1))*100 AS `part_failure`
FROM `re_wa_prod`.`re_ff_production_month_wise_part_trend_table_v1`
GROUP BY `MfgMonthNameYear`,
         `MfgMonthYear`
ORDER BY `MfgMonthYear` ASC

-- 



-- FF MONTHLY UPDATION

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a
SET
    a.FF1Maturity = b.FF1Maturity
FROM (SELECT a.MfgMonthYear, b.FF1Maturity
    FROM    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table_v1` a 
      INNER JOIN
          `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table_v2` b
      ON      
        a.MfgMonthYear = b.MfgMonthYear AND 
    WHERE   
        a.FF1Maturity != b.FF1Maturity) b
WHERE
    a.MfgMonthYear = b.MfgMonthYear;


UPDATE `pfam31.uniprot` a
SET a.auto_architecture = b.auto_architecture
FROM `pfam31.uniprot_architecture` b
WHERE a.uniprot_acc = b.uniprot_acc

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.FF3Maturity = b.FF3Maturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear;

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.FF6Maturity = b.FF6Maturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear;

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.FF12Maturity = b.FF12Maturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear;

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.FF18Maturity = b.FF18Maturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear;

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.FF24Maturity = b.FF24Maturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear;

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.FF30Maturity = b.FF30Maturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear;

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.FF36Maturity = b.FF36Maturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear;


-- EO Monthly Updation
UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_wa_prod.re_engine_opening_production_month_wise_table_v1` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.FirstYearMaturity = b.EOFirstYearMaturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear AND a.FirstYearMaturity = "NotMatured" AND b.EOFirstYearMaturity = "Matured";

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_wa_prod.re_engine_opening_production_month_wise_table_v1` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.SecondYearMaturity = b.EOSecondYearMaturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear;

UPDATE
    `re-warranty-analytics-prod.re_wa_prod.re_wa_prod.re_engine_opening_production_month_wise_table_v1` a,
    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
SET
    a.ThirdYearMaturity = b.EOThirdYearMaturity
WHERE
    a.MfgMonthYear = b.MfgMonthYear;




SELECT  a.MfgMonthYear, b.FF1Maturity
FROM    `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table_v1` a 
INNER JOIN
        `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table_v2` b
ON      a.MfgMonthYear = b.MfgMonthYear
WHERE   a.FF1Maturity != b.FF1Maturity







-- 
DMS JC TO NEW WARRANTY COST BASED ON CASHIER INVOICE DATE

SELECT a.VehicleChassisNumber, a.JobCard, a.PartNumber, a.ItemType, a.PrimaryConsequential, a.PartDescription, 
a.PartGroup, a.PartCategory, a.PartVariant, a.Zone, a.Region, a.State, a.DealerName, a.DealerCode, a.City, a.Model, 
a.ModelType, a.Plant, a.MfgMonth, a.MfgYear, a.LineAmount, a.FirstYearBucket, a.SecondYearBucket, a.ThirdYearBucket, 
a.MfgMonthNameYear, a.MfgMonthYear, a.DMart, b.EOFirstYearMaturity as FirstYearMaturity, b.EOSecondYearMaturity as SecondYearMaturity, b.EOThirdYearMaturity as ThirdYearMaturity,
a.PiperrInDate
FROM
(
SELECT VehicleChassisNumber, JobCard, PartNumber, ItemType, PrimaryConsequential, PartDescription, PartGroup, PartCategory,PartVariant, Zone,  Region,  
State, DealerName, DealerCode, upper(City) as City, Model, ModelType, Plant, MfgMonth, MfgYear, LineAmount,
IF((CreatedOn <= DATE_ADD(MfgDate, INTERVAL 1 Year)), 'first_year', 'NA') AS FirstYearBucket,
IF((CreatedOn <= DATE_ADD(MfgDate, INTERVAL 2 Year)), 'second_year', 'NA') AS SecondYearBucket,
IF((CreatedOn <= DATE_ADD(MfgDate, INTERVAL 3 Year)), 'third_year', 'NA') AS ThirdYearBucket,
FORMAT_DATE("%b-%Y", DATE(MfgYear, MfgMonth, 1)) as MfgMonthNameYear,
FORMAT_DATE("%Y-%m", DATE(MfgYear, MfgMonth, 1)) as MfgMonthYear,
"wc data" as DMart,
current_date() as PiperrInDate,
FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE1}` 
where MfgDate > '2019-01-01' and BillType like 'WARRANTY' 
and length(VehicleChassisNumber)=17 and length(JobCard) = 16 and Model not in ('U','NA','MACHISMO', '', 'TAURUS','ROYAL','DSW')
and Plant not in ('NA') and ModelType not in ('NA') and Country like 'Domestic' and PartAction is null
and CreatedOn <= DATE_ADD(MfgDate, INTERVAL 3 Year)
) a





-- DMS JC PANDAS TOO QUERY CONVERSION
-- CREATE TABLE `western-rider-354707.re_dev.dms_test` AS 
SELECT
h.JobCard, h.PartNumber, h.LineAmount, h.PartDescription, h.ItemType, h.Observation, h.RequestedQuantity, h.IssuedQuantityNew, h.IsRepeated, h.PartDefectID, h.PartDefectCode, h.PrimaryConsequential, h.PrimaryPart,h.PrimaryPartNumber, h.PrimaryPartGroup, h.PartAction, h.StatusReason, h.CreatedOnDateTime, h.CustomerComplaint, h.CustomerVoice, h.CashierInvoicingDateTime,  
h.Region, h.RegistrationNumber, h.VehicleChassisNumber, 
h.JobType, h.JobSubType, h.Kilometers, h.State, h.Zone, h.Name_store_account AS DealerName, h.FinalCustomer, h.BillType, h.DateofSale, h.Product, h.RepairCategory, 
h.part_batch_code, h.ModelID, h.DealerCode, h.PARTPRICE AS FieldObservationReport, h.TechnicalObservation, h.ComplaintObservedByDealer, h.CorrectiveActiontaken, h.JobCardStatus, h.CreatedDate AS CreatedOn,
EXTRACT(YEAR FROM h.CreatedDate) AS CreatedOnYear,
EXTRACT(MONTH FROM h.CreatedDate) AS CreatedOnMonth,
h.PiperrInDate,h.Model, h.ModelType, h.ModelName, h.Plant, h.MfgMonth,
h.MfgYear, h.Country, h.KmsGroup, h.PartGroup, h.PartVariant, h.PartCategory, j.City
FROM
(SELECT f.JobCard, f.PartNumber, f.LineAmount, f.PartDescription, f.ItemType, f.Observation, f.RequestedQuantity, f.IssuedQuantityNew, f.IsRepeated, f.PartDefectID, f.PartDefectCode, f.PrimaryConsequential, f.PrimaryPart, f.PrimaryPartNumber, f.PartAction, f.StatusReason, f.CreatedOn, 
f.CustomerComplaint, f.CustomerVoice, f.Region, f.RegistrationNumber, f.VehicleChassisNumber, 
f.JobType, f.JobSubType, f.Kilometers, f.State, f.Zone, f.Name_store_account, f.FinalCustomer, f.BillType, f.Product, f.RepairCategory, 
f.part_batch_code, f.Model, f.DealerCode, f.PARTPRICE, f.TechnicalObservation, f.ComplaintObservedByDealer, f.CorrectiveActiontaken, f.JobCardStatus, f.CreatedOnDateTime, f.CashierInvoicingDateTime, f.DateofSale,
CreatedDate, f.PiperrInDate, f.ModelType, f.ModelName, f.Plant, f.MfgMonth,f.ModelID,
f.MfgYear, f.Country, f.KmsGroup, f.PartGroup, f.PartCategory, f.PartVariant, g.PartGroup AS PrimaryPartGroup FROM
(SELECT d.JobCard, d.PartNumber, d.LineAmount, d.PartDescription, d.ItemType, d.Observation, d.RequestedQuantity, d.IssuedQuantityNew, d.IsRepeated, d.PartDefectID, d.PartDefectCode, d.PrimaryConsequential, d.PrimaryPart, d.PrimaryPartNumber, d.PartAction, d.StatusReason, d.CreatedOn, 
d.CustomerComplaint, d.CustomerVoice, d.Region, d.RegistrationNumber, d.VehicleChassisNumber, 
d.JobType, d.JobSubType, d.Kilometers, d.State, d.Zone, d.Name_store_account, d.FinalCustomer, d.BillType, d.Product, d.RepairCategory, d.ModelID,
d.part_batch_code, d.Model, d.DealerCode, d.PARTPRICE, d.TechnicalObservation, d.ComplaintObservedByDealer, d.CorrectiveActiontaken, d.JobCardStatus, d.CreatedOnDateTime, d.CashierInvoicingDateTime, d.DateofSale,
CreatedDate, d.PiperrInDate, d.ModelType, d.ModelName, d.Plant, d.MfgMonth,
d.MfgYear, d.Country, d.KmsGroup, e.PartGroup, e.PartCategory,e.PartVariant
FROM
(SELECT c.JobCard, c.PartNumber, c.LineAmount, c.PartDescription, c.ItemType, c.Observation, c.RequestedQuantity, c.IssuedQuantityNew, 
c.IsRepeated, c.PartDefectID, c.PartDefectCode, c.PrimaryConsequential, c.PrimaryPart, c.PrimaryPartNumber, c.PartAction, c.StatusReason, c.CreatedOn, c.CustomerComplaint, c.CustomerVoice, c.Region, c.RegistrationNumber, c.VehicleChassisNumber, c.
JobType, c.JobSubType, c.Kms AS Kilometers, c.State, c.Zone, c.Name_store_account, c.FinalCustomer, c.BillType, c.Product, c.RepairCategory, c.ModelID,
c.part_batch_code, c.Model, c.DealerCode, c.PARTPRICE, c.TechnicalObservation, c.ComplaintObservedByDealer, c.CorrectiveActiontaken, c.JobCardStatus, c.CreatedOnDateTime, c.CashierInvoicingDateTime, c.DateofSale,
CreatedDate, c.PiperrInDate, c.ModelType, c.ModelName, c.Plant, c.MfgMonth,
c.MfgYear, c.Country,
CASE  WHEN c.Kms >= 0 AND c.Kms <= 500
        THEN "(A) 0 - 500"
      WHEN c.Kms > 500 and c.Kms <= 3000
        THEN "(B) 500 - 3000"
      WHEN c.Kms > 3000 and c.Kms <= 5000
        THEN "(C) 3000 - 5000"
      WHEN c.Kms > 5000 and c.Kms <= 10000
        THEN "(D) 5000 - 10000"
      WHEN c.Kms > 10000 and c.Kms <= 15000
        THEN "(E) 10000 - 15000"
      WHEN c.Kms > 15000 and c.Kms <= 20000
        THEN "(F) 15000 - 20000"
      WHEN c.Kms > 20000 and c.Kms <= 25000
        THEN "(G) 20000 - 25000"
      WHEN c.Kms > 25000 and c.Kms <= 30000
        THEN "(H) 25000 - 30000"
      WHEN c.Kms > 30000
        THEN "(I) 30000 & Above"
      ELSE "NA"
END AS KmsGroup
FROM
    (SELECT b.JobCard, b.PartNumber, b.LineAmount, b.PartDescription, b.ItemType, b.Observation, b.RequestedQuantity, b.IssuedQuantityNew, 
    b.IsRepeated, b.PartDefectID, b.PartDefectCode, b.PrimaryConsequential, b.PrimaryPart, b.PrimaryPartNumber, b.PartAction, b.
    StatusReason, b.CreatedOn, b.CustomerComplaint, b.CustomerVoice, b.Region, b.RegistrationNumber, b.VehicleChassisNumber, b.
    JobType, b.JobSubType, b.Kilometers, b.State, b.Zone, b.Name_store_account, b.FinalCustomer, b.BillType, b.Product, b.RepairCategory, b.
    part_batch_code, b.Model,b.ModelID,
    b.DealerCode, b.PARTPRICE, b.TechnicalObservation, b.
    ComplaintObservedByDealer, b.CorrectiveActiontaken, b.JobCardStatus, b.CreatedOnDateTime, b.CashierInvoicingDateTime, b.DateofSale,
    b.CreatedDate, b.PiperrInDate, b.U_OR_D, b.CC, b.PlantOrMonth, b.ManufacturingYear, b.MonthOrPlant, b.ModelType, b.ModelName, b.Plant, b.MfgMonth,
    b.MfgYear,
    DATE(CAST(b.MfgYear AS INT64),CAST(b.MfgMonth AS INT64), 1),
    CASE  WHEN b.CC = '3' OR b.CC = "4" OR b.CC = '5' OR b.CC = '6' OR b.CC = '7'
            THEN "Domestic"
          ELSE "International"
    END AS Country,
    CASE  WHEN b.Kilometers IS NULL
            THEN -1
          ELSE b.Kilometers
    END AS Kms
        FROM
        (SELECT a.JobCard, a.PartNumber, a.LineAmount, a.PartDescription, a.ItemType, a.Observation, a.RequestedQuantity, a.IssuedQuantityNew, 
        a.IsRepeated, a.PartDefectID, a.PartDefectCode, a.PrimaryConsequential, a.PrimaryPart, a.PrimaryPartNumber, a.PartAction, a.
        StatusReason, a.CreatedOn, a.CustomerComplaint, a.CustomerVoice, a.Region, a.RegistrationNumber, a.VehicleChassisNumber, a.
        JobType, a.JobSubType, a.Kilometers, a.State, a.Zone, a.Name_store_account, a.FinalCustomer, a.BillType, a.Product, a.RepairCategory, a.
        part_batch_code, a.Model,
        DealerCode, a.PARTPRICE, a.TechnicalObservation, a.
        ComplaintObservedByDealer, a.CorrectiveActiontaken, a.JobCardStatus, a.CreatedOnDateTime, a.CashierInvoicingDateTime, a.DateofSale, a.ModelId,
        a.CreatedDate, a.PiperrInDate, a.U_OR_D, a.CC, a.PlantOrMonth, a.ManufacturingYear, a.MonthOrPlant,
        CASE WHEN LENGTH(VehicleChassisNumber) = 17 and Product IS NOT NULL
                THEN (
                      CASE 
                          WHEN a.U_OR_D = 'U'
                            THEN "UCE"
                          WHEN a.U_OR_D = 'D'
                            THEN "HIMALAYAN"
                          WHEN a.U_OR_D = 'P'
                            THEN "TWINS"
                          WHEN a.U_OR_D = 'J' AND CONTAINS_SUBSTR(Model, "CLASSIC")
                            THEN "NEW CLASSIC"
                          WHEN a.U_OR_D = "J" AND CONTAINS_SUBSTR(Product, "CLASSIC")
                            THEN a.Model 
                          WHEN a.U_OR_D = 'J'
                            THEN "METEOR"
                          ELSE
                            "NA" END  
                  )
        END AS ModelType,
        CASE  WHEN a.CC = '3'
                THEN CONCAT(a.Model," ","350")
              WHEN a.CC = '4'
                THEN CONCAT(a.Model," ","410")
              WHEN a.CC = '5'
                THEN CONCAT(a.Model," ","500")
              WHEN a.CC = '6'
                THEN CONCAT(a.Model," ","535")
              WHEN a.CC = '7'
                THEN CONCAT(a.Model," ","650")
              ELSE "NA"
        END AS ModelName,
        CASE  WHEN a.U_OR_D = 'U' OR a.U_OR_D = 'D'
                THEN (
                  CASE WHEN a.PlantOrMonth = 'A'
                    THEN "TVT"
                  WHEN a.PlantOrMonth = '1'
                    THEN "ORG"
                  WHEN a.PlantOrMonth = '2'
                    THEN "VLM"
                  ELSE "NA"
                  END
                )
              WHEN a.U_OR_D = 'P' OR a.U_OR_D = 'J'
                THEN (
                  CASE WHEN a.MonthOrPlant = '0'
                    THEN "TVT"
                  WHEN a.MonthOrPlant = '1'
                    THEN "ORG"
                  WHEN a.MonthOrPlant = '2'
                    THEN "VLM"
                  ELSE "NA"
                  END
                )
              ELSE "NA"
        END AS Plant,
        CASE  WHEN a.U_OR_D = 'U' OR a.U_OR_D = 'D'
                THEN (
                  CASE WHEN a.PlantOrMonth = 'A'
                    THEN "1"
                  WHEN a.PlantOrMonth = 'B'
                    THEN "2"
                  WHEN a.PlantOrMonth = 'C'
                    THEN "3"
                  WHEN a.PlantOrMonth = 'D'
                    THEN "4"
                  WHEN a.PlantOrMonth = 'E'
                    THEN "5"
                  WHEN a.PlantOrMonth = 'F'
                    THEN "6"
                  WHEN a.PlantOrMonth = 'G'
                    THEN "7"
                  WHEN a.PlantOrMonth = 'H'
                    THEN "8"
                  WHEN a.PlantOrMonth = 'K'
                    THEN "9"
                  WHEN a.PlantOrMonth = 'L'
                    THEN "10"
                  WHEN a.PlantOrMonth = 'M'
                    THEN "11"
                  WHEN a.PlantOrMonth = 'N'
                    THEN "12"
                  ELSE "NA"
                  END
                )
              WHEN a.U_OR_D = 'P' OR a.U_OR_D = 'J'
                THEN (
                  CASE WHEN a.MonthOrPlant = 'A'
                    THEN "1"
                  WHEN a.MonthOrPlant = 'B'
                    THEN "2"
                  WHEN a.MonthOrPlant = 'C'
                    THEN "3"
                  WHEN a.MonthOrPlant = 'D'
                    THEN "4"
                  WHEN a.MonthOrPlant = 'E'
                    THEN "5"
                  WHEN a.MonthOrPlant = 'F'
                    THEN "6"
                  WHEN a.MonthOrPlant = 'G'
                    THEN "7"
                  WHEN a.MonthOrPlant = 'H'
                    THEN "8"
                  WHEN a.MonthOrPlant = 'K'
                    THEN "9"
                  WHEN a.MonthOrPlant = 'L'
                    THEN "10"
                  WHEN a.MonthOrPlant = 'M'
                    THEN "11"
                  WHEN a.MonthOrPlant = 'N'
                    THEN "12"
                  ELSE "NA"
                  END
                )
              ELSE "NA"
        END AS MfgMonth,
        CASE  WHEN a.ManufacturingYear IS NOT NULL
                THEN (
                  CASE WHEN a.ManufacturingYear = 'A'
                    THEN "2010"
                  WHEN a.ManufacturingYear = 'B'
                    THEN "2011"
                  WHEN a.ManufacturingYear = 'C'
                    THEN "2012"
                  WHEN a.ManufacturingYear = 'D'
                    THEN "2013"
                  WHEN a.ManufacturingYear = 'E'
                    THEN "2014"
                  WHEN a.ManufacturingYear = 'F'
                    THEN "2015"
                  WHEN a.ManufacturingYear = 'G'
                    THEN "2016"
                  WHEN a.ManufacturingYear = 'H'
                    THEN "2017"
                  WHEN a.ManufacturingYear = 'J'
                    THEN "2018"
                  WHEN a.ManufacturingYear = 'K'
                    THEN "2019"
                  WHEN a.ManufacturingYear = 'L'
                    THEN "2020"
                  WHEN a.ManufacturingYear = 'M'
                    THEN "2021"
                  WHEN a.ManufacturingYear = 'N'
                    THEN "2022"
                  ELSE "1900"
                  END
                )
        END AS MfgYear,          
        FROM
            (SELECT JobCard, PartNumber, LineAmount, PartDescription, ItemType, Observation, RequestedQuantity, IssuedQuantityNew, 
            IsRepeated, PartDefectID, PartDefectCode, PrimaryConsequential, PrimaryPart, PrimaryPartNumber, PartAction, 
            StatusReason, CreatedOn, CustomerComplaint, CustomerVoice, Region, RegistrationNumber, 
            UPPER(VehicleChassisNumber) AS VehicleChassisNumber, JobType, JobSubType, Kilometers, State, Zone, DealerName, FinalCustomer, Model AS ModelID,
            BillType, UPPER(Product) AS Product, RepairCategory, part_batch_code, 
            CASE  WHEN UPPER(LEFT(Product, 1)) IS NULL 
                    THEN "NA" 
                  WHEN CONTAINS_SUBSTR(UPPER(LEFT(Product, 1)), "CONTINENTAL")
                    THEN "CONTINENTAL"
                  WHEN UPPER(LEFT(Product, 1)) = "CL" OR UPPER(LEFT(Product, 1)) = "SIGNALS"
                    THEN "CLASSIC"
                  ELSE UPPER(LEFT(Product, 1)) 
            END AS Model, 
            DealerCode, FieldObservationReport, TechnicalObservation, 
            ComplaintObservedByDealer, CorrectiveActiontaken, JobCardStatus,
            -- CAST(CreatedOn AS DATETIME) CreatedOnDateTime, CAST(CashierInvoicingDateTime AS DATETIME) CashierInvoicingDateTime,
            -- CAST(DateofSale AS DATETIME) DateofSale, CAST(CreatedOn AS DATE) CreatedDate,
            CURRENT_DATE() AS PiperrInDate, 
            SUBSTRING(VehicleChassisNumber, 3, 4) AS U_OR_D, SUBSTRING(VehicleChassisNumber, 4, 5) AS CC, SUBSTRING(VehicleChassisNumber, 8, 9) AS PlantOrMonth,
            SUBSTRING(VehicleChassisNumber, 9, 10) AS ManufacturingYear, SUBSTRING(VehicleChassisNumber, 10, 11) AS MonthOrPlant
        FROM `western-rider-354707.re_dev.dms_rawdata`) a) b) c) d
LEFT JOIN `western-rider-354707.re_dev.dev_part_group` e
ON
d.ModelType = e.ModelType AND d.PartNumber = e.PartNo) f
LEFT JOIN `western-rider-354707.re_dev.dev_part_group` g
ON
f.ModelType = g.ModelType AND f.PrimaryPartNumber = g.PartNo) h
LEFT JOIN `western-rider-354707.re_dev.dev_dealer_master` j
ON
h.DealerCode = j.DealerCode





REFACTORING QUERY

-- Model

CASE WHEN UPPER(SPLIT(Product, ' ')[OFFSET(0)]) IS NULL 
                    THEN "NA"
                  WHEN CONTAINS_SUBSTR(UPPER(SPLIT(Product, " ")[OFFSET(0)]), "CONTINENTAL")
                    THEN "CONTINENTAL"
                  WHEN UPPER(SPLIT(Product, ' ')[OFFSET(0)]) = "CL" OR UPPER(SPLIT(Product, ' ')[OFFSET(0)]) = "SIGNALS"
                    THEN "CLASSIC"
                  WHEN CONTAINS_SUBSTR(UPPER(SPLIT(Product, ' ')[OFFSET(0)]), "HIMALAYAN")
                    THEN "HIMALAYAN"
                  WHEN CONTAINS_SUBSTR(UPPER(SPLIT(Product, ' ')[OFFSET(0)]), "INTERCEPTOR")
                    THEN "INTERCEPTOR"
                  ELSE REGEXP_EXTRACT_ALL(UPPER(Product), r'[a-zA-Z]+')[OFFSET(0)] 
            END AS Model 


-- cc and u_or_d
SUBSTRING(VehicleChassisNumber, 4, 1) AS U_OR_D, SUBSTRING(VehicleChassisNumber, 5, 1) AS CC, SUBSTRING(VehicleChassisNumber, 9, 1) AS PlantOrMonth,
SUBSTRING(VehicleChassisNumber, 10, 1) AS ManufacturingYear, SUBSTRING(VehicleChassisNumber, 11, 1) AS MonthOrPlant




-- ModelType ModelName
CASE WHEN LENGTH(VehicleChassisNumber) = 17 and Product IS NOT NULL
                THEN (
                      CASE 
                          WHEN b.U_OR_D = 'U'
                            THEN "UCE"
                          WHEN b.U_OR_D = 'D'
                            THEN "HIMALAYAN"
                          WHEN b.U_OR_D = 'P'
                            THEN "TWINS"
                          WHEN b.U_OR_D = 'J' AND CONTAINS_SUBSTR(Model, "CLASSIC")
                            THEN "NEW CLASSIC"
                          WHEN b.U_OR_D = "J" AND CONTAINS_SUBSTR(Product, "CLASSIC")
                            THEN b.Model 
                          WHEN b.U_OR_D = 'J'
                            THEN "METEOR"
                          ELSE
                            "NA" END  
                  )
                ELSE
                  "NA"
        END AS ModelType,
        CASE  WHEN b.CC = '3'
                THEN CONCAT(b.Model," ","350")
              WHEN b.CC = '4'
                THEN CONCAT(b.Model," ","410")
              WHEN b.CC = '5'
                THEN CONCAT(b.Model," ","500")
              WHEN b.CC = '6'
                THEN CONCAT(b.Model," ","535")
              WHEN b.CC = '7'
                THEN CONCAT(b.Model," ","650")
              ELSE "NA"
        END AS ModelName,

-- PLANT

CASE  WHEN b.U_OR_D = 'U' OR b.U_OR_D = 'D'
                THEN (
                  CASE WHEN b.PlantOrMonth = '0'
                    THEN "TVT"
                  WHEN b.PlantOrMonth = '1'
                    THEN "ORG"
                  WHEN b.PlantOrMonth = '2'
                    THEN "VLM"
                  ELSE "NA"
                  END
                )
              WHEN b.U_OR_D = 'P' OR b.U_OR_D = 'J'
                THEN (
                  CASE WHEN b.MonthOrPlant = '0'
                    THEN "TVT"
                  WHEN b.MonthOrPlant = '1'
                    THEN "ORG"
                  WHEN b.MonthOrPlant = '2'
                    THEN "VLM"
                  ELSE "NA"
                  END
                )
              ELSE "NA"
        END AS Plant

-- MfgMonth

CASE  WHEN b.U_OR_D = 'P' OR b.U_OR_D = 'J'
                THEN (
                  CASE WHEN b.PlantOrMonth = 'A'
                    THEN "1"
                  WHEN b.PlantOrMonth = 'B'
                    THEN "2"
                  WHEN b.PlantOrMonth = 'C'
                    THEN "3"
                  WHEN b.PlantOrMonth = 'D'
                    THEN "4"
                  WHEN b.PlantOrMonth = 'E'
                    THEN "5"
                  WHEN b.PlantOrMonth = 'F'
                    THEN "6"
                  WHEN b.PlantOrMonth = 'G'
                    THEN "7"
                  WHEN b.PlantOrMonth = 'H'
                    THEN "8"
                  WHEN b.PlantOrMonth = 'K'
                    THEN "9"
                  WHEN b.PlantOrMonth = 'L'
                    THEN "10"
                  WHEN b.PlantOrMonth = 'M'
                    THEN "11"
                  WHEN b.PlantOrMonth = 'N'
                    THEN "12"
                  ELSE "1"
                  END
                )
              WHEN b.U_OR_D = 'U' OR b.U_OR_D = 'D'
                THEN (
                  CASE WHEN b.MonthOrPlant = '0'
                    THEN "1"
                  WHEN b.MonthOrPlant = 'B'
                    THEN "2"
                  WHEN b.MonthOrPlant = 'C'
                    THEN "3"
                  WHEN b.MonthOrPlant = 'D'
                    THEN "4"
                  WHEN b.MonthOrPlant = 'E'
                    THEN "5"
                  WHEN b.MonthOrPlant = 'F'
                    THEN "6"
                  WHEN b.MonthOrPlant = 'G'
                    THEN "7"
                  WHEN b.MonthOrPlant = 'H'
                    THEN "8"
                  WHEN b.MonthOrPlant = 'K'
                    THEN "9"
                  WHEN b.MonthOrPlant = 'L'
                    THEN "10"
                  WHEN b.MonthOrPlant = 'M'
                    THEN "11"
                  WHEN b.MonthOrPlant = 'N'
                    THEN "12"
                  ELSE "1"
                  END
                )
              ELSE "1"
END AS MfgMonth


-- WARRANTY COST

SELECT a.VehicleChassisNumber, a.JobCard, a.PartNumber, a.ItemType, a.PrimaryConsequential, a.PartDescription, 
a.PartGroup, a.PartCategory, a.PartVariant, a.Zone, a.Region, a.State, a.DealerName, a.DealerCode, a.City, a.Model, 
a.ModelType, a.Plant, a.MfgMonth, a.MfgYear, a.LineAmount, a.FirstYearBucket, a.SecondYearBucket, a.ThirdYearBucket, 
a.MfgMonthNameYear, a.MfgMonthYear, a.DMart, b.EOFirstYearMaturity as FirstYearMaturity, b.EOSecondYearMaturity as SecondYearMaturity, b.EOThirdYearMaturity as ThirdYearMaturity,
a.PiperrInDate
FROM
(
SELECT VehicleChassisNumber, JobCard, PartNumber, ItemType, PrimaryConsequential, PartDescription, PartGroup, PartCategory,PartVariant, Zone,  Region,  
State, DealerName, DealerCode, upper(City) as City, Model, ModelType, Plant, MfgMonth, MfgYear, LineAmount,
IF((CreatedOn <= DATE_ADD(MfgDate, INTERVAL 1 Year)), 'first_year', 'NA') AS FirstYearBucket,
IF((CreatedOn <= DATE_ADD(MfgDate, INTERVAL 2 Year)), 'second_year', 'NA') AS SecondYearBucket,
IF((CreatedOn <= DATE_ADD(MfgDate, INTERVAL 3 Year)), 'third_year', 'NA') AS ThirdYearBucket,
FORMAT_DATE("%b-%Y", DATE(MfgYear, MfgMonth, 1)) as MfgMonthNameYear,
FORMAT_DATE("%Y-%m", DATE(MfgYear, MfgMonth, 1)) as MfgMonthYear,
"wc data" as DMart,
current_date() as PiperrInDate,
FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE1}` 
where MfgDate > '2019-01-01' and BillType like 'WARRANTY' 
and length(VehicleChassisNumber)=17 and length(JobCard) = 16 and Model not in ('U','NA','MACHISMO', '', 'TAURUS','ROYAL','DSW')
and Plant not in ('NA') and ModelType not in ('NA') and Country like 'Domestic' and PartAction is null
and CreatedOn <= DATE_ADD(MfgDate, INTERVAL 3 Year)
) a
LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.{BQ_MATURED_TABLE}` b
ON
a.MfgMonthYear = b.MfgMonthYear and a.ModelType=b.ModelGroup


-- RSA
-- RSA
SELECT CaseNo, IncidentDate,	City,	State,	Region,	CustomerName,	
ContactNumber,	VehicleChassisNumber,	Model,	Kilometers,	
BreakdownReason,	IF (CONTAINS_SUBSTR(TypeofIncident, "ENGINE"),
"ENGINE RELATED", TypeofIncident) AS TypeofIncident,	PartReplaced,	
ActionTaken,	IF (CONTAINS_SUBSTR(ServiceProvided, "TOW"),"TOWING", 
ServiceProvided) AS ServiceProvided	,DealerName,	JobCard,	ModelType,	
PiperrInDate,	IncidentYear,	IncidentMonth	,IncidentMonthNameYear, IncidentMonthYear 
FROM
(SELECT CaseNo, IncidentDate, City, State, Region, CustomerName, ContactNumber, 
VehicleChassisNumber, Model, Kilometers, BreakdownReason, 
UPPER(TypeofIncident) AS TypeofIncident, PartReplaced, ActionTaken, 
UPPER(ServiceProvided) AS ServiceProvided, DealerName, JobCard, ModelType, 
PiperrInDate, IncidentYear, IncidentMonth, IncidentMonthNameYear, IncidentMonthYear
FROM 
`re-warranty-analytics-prod.re_wa_prod.re_rsa_table` 
) 

-- CALL CENTRE
SELECT * FROM ``