SELECT t1.f1 as MfgMonthYear, t1.ModelGroup,t1.PCount as PCount,t2.Scount as SCount,
t2.FF1Count as FF1Count, t2.FF3Count as FF3Count, t2.FF6Count as FF6Count, t2.FF12Count as FF12Count, t2.FF18Count as FF18Count, t2.FF24Count as FF24Count,
t2.FF30Count as FF30Count, t2.FF36Count as FF36Count,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF1Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF1Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF3Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF3Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF6Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF6Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF12Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF12Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF18Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF18Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF24Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF24Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF30Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF30Maturity,
IF(t2.Scount >= 0.95*t1.PCount AND t2.FF36Count >= 0.95*t2.Scount, "Matured", "NotMatured") AS FF36Maturity,
t2.FF1, t2.FF3, t2.FF6, t2.FF12, t2.FF18, t2.FF24, t2.FF30, t2.FF36
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
'(1) FF1' AS FF1,
'(2) FF3' AS FF3,
'(3) FF6' AS FF6,
'(4) FF12' AS FF12,
'(5) FF18' AS FF18,
'(6) FF24' AS FF24,
'(7) FF30' AS FF30,
'(8) FF36' AS FF36
FROM `re-warranty-analytics-prod.re_wa_prod.sales_mat_table`
WHERE ModelGroup != 'NA'
group by f1, ModelGroup
order by 1
) t2
ON t1.f1 = t2.f1 and t1.ModelGroup=t2.ModelGroup
where t1.f1 >= "2019-01" and t1.f1 <= FORMAT_DATE("%Y-%m", CURRENT_DATE()) and t1.ModelGroup is not null
order by t1.f1;

#####################
SELECT  
a.VehicleChassisNumber, a.JobCard, a.PartNumber, a.PartDescription, a.PartGroup, a.PartVariant, a.PartCategory, a.PartDefectCode, a.Zone, a.Region, a.State, a.DealerCode, a.DealerName, a.City, a.Model, a.ModelType, a.Plant, a.MfgMonth, a.MfgYear, a.FF1, a.FF3, a.FF6, a.FF12, a.FF18, a.FF24, a.FF30, a.FF36, a.MfgMonthNameYear, a.MfgMonthYear, a.DMart, a.PiperrInDate, b.FF1Maturity, b.FF3Maturity, b.FF6Maturity, b.FF12Maturity, b.FF18Maturity, b.FF24Maturity, b.FF30Maturity, b.FF36Maturity
FROM `re-warranty-analytics-prod.re_wa_prod.re_ff_production_month_wise_table_v2` a
LEFT JOIN `re-warranty-analytics-prod.re_wa_prod.rematurity_marker_table` b
on a.MfgMonthYear = b.MfgMonthYear and a.ModelType = b.ModelGroup;



mat_v1 --> mat_v2 