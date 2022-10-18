
CREATE or replace TABLE `re-warranty-analytics-prod.re_wa_prod.re_service_quality_usecase_table_v1` AS
with cte as
(
SELECT
VehicleChassisNumber, CreatedOn, JobCard, PartNumber, PartDescription,  ItemType,  PartAction, PartGroup,  PartVariant, PartCategory, PartDefectCode,  JobSubType, JobCardStatus,  BillType, IssuedQuantityNew, CustomerComplaint, 
Zone, Region, State, DealerCode, DealerName, City, Kilometers,  KmsGroup, Model,  ModelType,  Plant,  MfgMonth,  MfgYear,  CreatedOnYear,  CreatedOnMonth
FROM
(
SELECT VehicleChassisNumber,CreatedOn,JobCard,PartNumber,PartDescription, ItemType, PartAction,PartGroup, PartVariant,PartCategory,PartDefectCode, JobSubType, JobCardStatus, BillType,IssuedQuantityNew,CustomerComplaint, 
Zone,Region,State,DealerCode,DealerName,City,Kilometers, KmsGroup, 
Model, ModelType, Plant, MfgMonth, MfgYear, CreatedOnYear, CreatedOnMonth
FROM `re-warranty-analytics-prod.re_wa_prod.re_dms_table_v2`
where 
ItemType like 'Part' and PartAction is null and JobSubType not in ('ACCIDENTAL','PDI') and JobCardStatus != 'Canceled' and length(VehicleChassisNumber)=17 and length(JobCard) = 16 and ModelType not in ('NA') and Model not in ('U','NA','MACHISMO', '', 'TAURUS','ROYAL','DSW') and Plant not in ('NA') and 
PartNumber is not null and PartNumber NOT IN (SELECT PartNo FROM `re-warranty-analytics-prod.re_wa_prod.re_wa_service_quality_part_exclude_table`) and PartDescription is not null and Country like 'Domestic' and 
MfgYear !=1900)),
cte2 as
(
select *,
row_number() over(partition by JobCard,PartNumber) as r_n
from cte
),
cte3 as 
(
select * from cte2
where r_n = 1
),
cte4 as 
(select *,
lag(CreatedOn) OVER(PARTITION BY VehicleChassisNumber, PartNumber order by CreatedOn,Kilometers) as prev_date,
lag(Kilometers) OVER(PARTITION BY VehicleChassisNumber, PartNumber order by CreatedOn,Kilometers) as prev_km,
lag(JobCard) OVER(PARTITION BY VehicleChassisNumber, PartNumber order by CreatedOn,Kilometers) as PreviousJC
FROM cte3),

cte5 as 
(select VehicleChassisNumber,CreatedOn,JobCard,PartNumber,PartDescription, ItemType, PartAction,PartGroup, PartVariant,PartCategory,PartDefectCode, JobSubType, JobCardStatus,BillType,IssuedQuantityNew,CustomerComplaint, 
Zone,Region,State,DealerCode,DealerName,City,Kilometers, KmsGroup, 
Model, ModelType, Plant, MfgMonth, MfgYear, CreatedOnYear, CreatedOnMonth, PreviousJC, 
IFNULL(DATE_DIFF(CreatedOn, prev_date, DAY), -1) as RepeatedDaysDifference, 
IFNULL(Kilometers- prev_km,-1) as RepeatedKilometersDifference,
FORMAT_DATE('%b-%Y', DATE(CreatedOn)) as JCMonthNameYear,
FORMAT_DATE('%Y-%m', DATE(CreatedOn))  as JCMonthYear,
CURRENT_DATE() as PiperrInDate,
FORMAT_DATE('%Y-%m', DATE(MfgYear, MfgMonth, 1))  as MfgMonthYear,
FORMAT_DATE('%b-%Y', DATE(MfgYear, MfgMonth, 1))  as MfgMonthNameYear
FROM cte4)
select * from cte5
where RepeatedDaysDifference BETWEEN 0 and 91 and RepeatedKilometersDifference BETWEEN 0 and 3000;
