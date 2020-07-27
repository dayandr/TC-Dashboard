Drop TABLE if exists dayandr.ReeferTLD;

create TABLE dayandr.ReeferTLD
as

SELECT 
                                TLD.loadnum as Loadnum,
        --TLD.Mode as Mode,
                                M.ModeNameSimple as ModeNameSimple,
        cast(TLD.EnteredDateTime as timestamp) as entereddatetime,
        Cast(TLD.EnteredDate as date) as entereddate,
        cast(TLD.ActivatedDateTime as timestamp) as activateddatetime,
        cast(TLD.activitydate as timestamp) as ActivityDate,
        cast(TLD.BookDateTime as timestamp) as bookdatetime,
        cast(TLD.BookDate as date) as bookdate,
       -- cast(CONCAT(CAST(SP.scheddateopen AS DATE),' ' ,coalesce(SUBSTR(SP.schedtimeopen,12 ),'00:00:00')) as timestamp) as PUAppt,
        --datediff(nvl((cast(CONCAT(CAST(SP.scheddateopen AS DATE),' ' ,coalesce(SUBSTR(SP.schedtimeopen,12 ),'00:00:00')) as timestamp)), cast(TLD.activitydate as timestamp)), TLD.activateddatetime)*24 as ActivationLeadTime,
        cast(TLD.PickCloseDateTime as timestamp) as pickclosedatetime,
        cast(TLD.PickCloseDate as date) as pickclosedate,
      --  cast(CONCAT(CAST(SD.scheddateclose AS DATE),' ' ,coalesce(SUBSTR(SD.schedtimeclose,12 ),'00:00:00')) as timestamp) as DELAppt,
       -- cast(SD.arriveddatetime as date) as DelArrivalDate,
        cast(TLD.CompletedDateTime as timestamp) as completeddatetime,
        cast(TLD.CompletedDate as date) as completeddate,
        cast(TLD.CancelDateTime as timestamp) as canceldatetime,
        cast(TLD.CancelDate as date) as canceldate,
        TLD.ActiveTimeToCancelMintues,
        TLD.ActiveTimeToCancel,
        TLD.EnteredTimeToCancelMintues,
        TLD.EnteredTimeToCancel,
        TLD.ActiveTimeToBookMinutes,
        TLD.ActiveTimeToBook,
        TLD.CarrierLeadTimeDays,
        TLD.CarrierLeadTime,
        TLD.CustomerLeadTimeDays,
        TLD.CustomerLeadTime,
       -- datediff(nvl(cast(SD.arriveddatetime as date), nvl(cast(sd.scheddateclose as Date),cast(sd.requesteddate as Date))),nvl(cast(sp.stopcompletedatetime as date),Cast(TLD.activitydate as date))) as TransitDays,
       -- nvl(cast(SD.arriveddatetime as date), nvl(cast(sd.scheddateclose as Date), cast(SD.requesteddate as Date))) as DeliveryDate,
        TLD.LoadActivationStatus,
        TLD.Rating,
        TLD.RateLevel,
        TLD.TeamFlag,
        TLD.HazMat,
        TLD.OverDimensionalYN,
        TLD.isHeavyHaul,
        TLD.Value,
        CASE WHEN TLD.Value > 100000 then 1 else 0 end as HVFlag,
        --TLD.MinTemp,
        --TLD.MaxTemp,
        TLD.StopCount,
        TLD.StopQuantity,
        TLD.`condition`,
        Case when lower(trim(TLD.`condition`)) = 'x' then 'Cancelled' else 'Executed' end as LoadType,
        --TLD.MaxStatus,
        TLD.CarrierReps,
        TLD.Books,
        TLD.CarrierBranch,
        TLD.CarrierCode,
        Ca.partyname as CarrierName,
        TLD.CarrierProgram,
        TLD.RespBranch,
        TLD.Bounces,
        TLD.Consol,
        TLD.LoadBookingOutcome,
        TLD.BillToCompCode,
        TLD.CustomerBranch,
        TLD.ControlRepBranch,
        TLD.CustomerRep,
        TLD.ControlRep,
        TLD.CarrierRep,
        TLD.BookedBy,
        TLD.Rate,
        TLD.LinehaulRate,
        TLD.FuelRate,
        TLD.TotalAccessorialRate,
        TLD.LHPlusFuelNormalizedRate,
        TLD.AllInNormalizedRate,
        TLD.Cost,
        TLD.LinehaulCost,
        TLD.FuelCost,
        TLD.TotalAccessorialCost,
        TLD.LHPlusFuelNormalizedCost,
        TLD.AllInNormalizedCost,
        TLD.`doefuel@chrmiles` as doefuelchrmiles,
        --TLD.PreHoldbackSpread,
        --TLD.HoldBackRev,
        TLD.Spread,
        --TLD.CustomerSpread,
        --TLD.CarrierSpread,
        --TLD.ControlSpread,
        --TLD.NegLoadYN,
        TLD.Miles,
        TLD.DropTrailerFlagPick,
        TLD.DropTrailerFlagDrop,
        TLD.DropTrailerPickDropType,
        Case When lower(trim(wo.loadschedreq)) = 'a' then 'Appointment'
                         When lower(trim(wo.loadschedreq)) = 'n' then 'Notice Call'
                                     When lower(trim(wo.loadschedreq)) = 'o' then 'Open' else '' end as PickSchedule,
                                Case When lower(trim(wd.unloadschedreq)) = 'a' then 'Appointment'
                         When lower(trim(wd.unloadschedreq)) = 'n' then 'Notice Call'
                                     When lower(trim(wd.unloadschedreq)) = 'o' then 'Open' else '' end as DropSchedule,
                                wo.loadwork as OriginLoadWork,
                                wd.unloadwork as DestUnloadWork,
                                wo.opentime as OriginOpenTime,
                                wo.closetime as OriginCloseTime,
                                wd.opentime as DestOpenTime,
                                wd.closetime as DestCloseTime,
        TLD.OriginWarehouseCode,
        TLD.OriginWarehouseName,
        TLD.DestinationWarehouseCode,
        TLD.DestinationWarehouseName,
        TLD.OZip,
        TLD.DZip,
        TLD.OZip3Digit,
        TLD.DZip3Digit,
        --TLD.OZip2Digit,
        --TLD.DZip2Digit,
        TLD.OriginCity,
        TLD.OriginState,
        TLD.OriginCityState,
        TLD.OriginStateCountry,
        TLD.OriginCountry,
        TLD.DestinationCity,
        TLD.DestinationState,
        TLD.DestinationCityState,
        TLD.DestinationStateCountry,
        TLD.DestinationCountry,
        TLD.Lane,
        TLD.`od state match yn`,
        --TLD.`total cross covered`,
        BA.primarybusinessline,
        BA.primarybusinesslineid,
        CASE When BA.primarybusinesslineid = 73 then 'Y' else 'N' end as TMCLoad,
        ER.rollupname as EAR2Name,
                                ERP.rollupid as EAR2ID,
                                CT.nrtier as NRTier,
                                DATO.datregion as DATOrigin,
                                DATO.datcitystate as DATCitySTOrig,
                                DATD.datregion as DATDest,
                                DATD.datcitystate as DATCitySTDest,
                                Concat(DATO.datregion,' - ',DATD.datregion) as DATCorridor,
                                Concat(DATO.datcitystate, ' - ',DATD.datcitystate) as DATCorridorCS,
                                CASE WHEN TLD.miles <250 THEN '<250'
                                                WHEN TLD.miles <350 then '251-349'
                                                WHEN TLD.miles <550 then '350-550'
                                                WHEN TLD.miles <1101 then '551-1100'
                                                Else '1100+' end as NonLocalLOH,
                                CASE WHEN TLD.miles <=10 THEN '<10'
                                                WHEN TLD.miles <=25 then '11-25'
                                                WHEN TLD.miles <=50 then '26-50'          
                                                WHEN TLD.miles <=75 then '51-75'
                                                Else '76+' end as LocalLOH,
                                CASE WHEN TLD.lhplusfuelnormalizedcost <500 THEN '<$500'
                                                WHEN TLD.lhplusfuelnormalizedcost <950 then '$500-949'
                                                WHEN TLD.lhplusfuelnormalizedcost <1051 then '$950-1050'
                                                WHEN TLD.lhplusfuelnormalizedcost <1899 then '$1051-1899'
                                                WHEN TLD.lhplusfuelnormalizedcost <2001 then '$1900-2000'
                                                WHEN TLD.lhplusfuelnormalizedcost <2400 then '$2001-2400'
                                                Else '$2400+' end as CostGroup,
                                CASE WHEN TLD.bookingrule =100 THEN '1 - Exempt'
                                                WHEN TLD.bookingrule =90 THEN '2 - Rep'
                                                WHEN TLD.bookingrule =20 THEN '3 - Key'
                                                WHEN TLD.bookingrule =10 THEN '4 - Core'
                                                ELSE '5 - Open' END AS BookingRule,
                                TLD.isregulatedbystf,
                                TLD.relationship,
                                TLD.isawarded,
                                TLD.maxbuy,
                                CASE WHEN TLD.maxbuy >= TLD.cost THEN '0'
                                                ELSE '1' END AS OverMaxBuy,
                                CASE WHEN TLD.maxbuy >= TLD.cost THEN 'NULL'
                                                ELSE TLD.cost-TLD.maxbuy END AS AmtOverMax,
                                CASE WHEN TLD.maxbuy >= TLD.cost THEN '1'
                                                ELSE '0' END AS UnderMaxBuy,
                                CASE WHEN TLD.maxbuy >= TLD.cost THEN TLD.maxbuy-TLD.cost
                                                ELSE 'NULL' END AS AmtUnderMax,

                                M.ModeClean,
                                TLD.totaloffers,
                                TLD.offersaccepted,
                                TLD.profitableoffers,
                                TLD.ismultileg,
                                V.sic4name, 
                                V.chrindustrysegmentname as IndustrySegment,
                                V.`sales description` as SizeSegment,
                                TLD.web
                                
                                                                
                                FROM dbreporting.bt_tld_loads_v2 TLD 
 -- INNER JOIN dbreporting.bt_load
  INNER JOIN dbreporting.bt_mode M ON UPPER(TRIM(TLD.Mode)) = UPPER(TRIM(M.Mode))
  --INNER JOIN EDW_OLAP.dbo.dimCustomer C (NOLOCK) ON TLD.BillToCompCode = C.CustomerPartyCode
  left Join edl.mdmreport_dbo_branchownership BO ON UPPER(TRIM(TLD.billtocompcode))= UPPER(TRIM(BO.partycode)) and BO.partytyperdn = 7
  left JOIN edl.mdmreport_dbo_branchaccounting BA  ON TRIM(TLD.customerbranch) = TRIM(BA.branchnumber)
--  Inner Join EDL.express_rap_dbo_stops SP on TLD.loadnum = SP.loadnum and Upper(TRIM(TLD.originwarehousecode)) = Upper(Trim(SP.warehousecode)) and SP.stoptype = 'P'
--  Inner Join EDL.express_rap_dbo_stops SD on TLD.loadnum = SD.loadnum and Upper(TRIM(TLD.destinationwarehousecode)) = Upper(Trim(SD.warehousecode)) and SD.stoptype = 'D'
  left Join t_self_service_nba_na.tld_datregions DATO on Upper(TRIM(TLD.ozip3digit)) = UPPER(TRIM(DATO.dat3zip))
  left Join t_self_service_nba_na.tld_datregions DATD on Upper(TRIM(TLD.dzip3digit)) = UPPER(TRIM(DATD.dat3zip))
  Left OUTER JOIN edl.mdmreport_dbo_carrier Ca on lower(Trim(TLD.carriercode)) = lower(trim(Ca.partycode))
  LEFT OUTER JOIN edl.mdmreport_dbo_enterpriserollupparty ERP on UPPER(TRIM(TLD.billtocompcode)) = UPPER(TRIM(ERP.partycode))
  LEFT OUTER JOIN edl.mdmreport_dbo_enterpriserollup ER on ERP.rollupid = ER.rollupid_pk
  LEFT OUTER Join edl.mdmreport_dbo_gtm_dbsegmentationdata V on  ER.rollupid_pk = V.rollupid_pk
  LEFT OUTER JOIN t_self_service_nba_na.nast_cust_tiers CT on Upper(Trim(ER.rollupname)) = Upper(Trim(CT.ear2name)) and CT.`year` in ('2019','2018','2020')
  LEFT OUTER JOIN dbreporting.dbreporting_dbo_report_express_warehouses wo on upper(trim(TLD.originwarehousecode)) = upper(trim(wo.companycode))
  Left Outer JOIN dbreporting.dbreporting_dbo_report_express_warehouses wd on upper(trim(TLD.destinationwarehousecode)) = upper(trim(wd.companycode))
  
  
    WHERE trim(lower(TLD.Condition)) NOT IN ('i','u') 
     and TLD.ActivityDate >= '2018-01-01 00:00:00' and TLD.Activitydate < CURRENT_DATE     
    --AND TLD.ActivityDate between add_months(date_add(date_sub(Cast(CURRENT_DATE as String),pmod(datediff(Cast(CURRENT_DATE as String),'1900-01-07'),7)),-1),-36) -- 12 months prior to last day of prior week
                                --TLD.ActivityDate between CAST(CONCAT_WS('-',CAST(YEAR(ADD_MONTHS(CURRENT_DATE,-13))AS VARCHAR(4)),CAST(MONTH(ADD_MONTHS(CURRENT_DATE,-12))AS VARCHAR(2)),'01') AS DATE) -- 1st day 12 months prior
                               -- and date_add(date_sub(Cast(CURRENT_DATE as String),pmod(datediff(Cast(CURRENT_DATE as String),'1900-01-07'),7)),-1) --last day of prior week
                --AND trim(TLD.StopQuantity) = '1P1D'
                --AND TLD.HazMat = 0
                --AND TLD.Mode NOT LIKE '%O%'
               AND UPPER(TRIM(M.ModeNameSimple)) IN ('REEFER')
                --AND TLD.TeamFlag = 0
                AND TLD.OriginCountry IN ('US','CA','MX')
               AND TLD.DestinationCountry IN ('US','CA','MX')
               AND BA.primarybusinesslineid in (62,73,65) --NAST/ManagedServices/Sourcing
               ;
               
 ----------------TLD Data     

Drop Table if exists dayandr.ReeferBounce;

Create Table dayandr.ReeferBounce
as
Select 
tld.Loadnum,
count(BO.BounceKey) as Bounces,
count(BD.BounceKey) as SameDayBounces

From dayandr.ReeferTLD TLD
Inner Join edl.express_rap_dbo_loadbooks lb on lb.loadnum = tld.loadnum
                    and lb.BookType = 'TL'
                    and lb.Bounced = 1
left join (Select distinct
       tld.Loadnum,
       concat(lb.CarrierCode, lb.bouncedDate) as BounceKey

       From dayandr.ReeferTLD TLD
       Inner Join edl.express_rap_dbo_loadbooks lb on lb.loadnum = tld.loadnum
                    and lb.BookType = 'TL'
                    and lb.Bounced = 1) BO on bo.loadnum = tld.loadnum
left join (Select distinct
       tld.Loadnum,
       concat(lb.CarrierCode, lb.bouncedDate) as BounceKey

       From dayandr.ReeferTLD TLD
       Inner Join edl.express_rap_dbo_loadbooks lb on lb.loadnum = tld.loadnum
                    and lb.BookType = 'TL'
                    and lb.Bounced = 1
                    and cast(lb.BouncedDate as timestamp) = cast(TLD.activitydate as timestamp)) BD on BD.loadnum = tld.loadnum

Group by tld.Loadnum
;

--Drop table if  exists dayandr.ReeferCostQuote;
  
--create table dayandr.ReeferCostQuote               
--as
--Select 
--                                T.Loadnum,
--                                Max(T.loadprogramtype) as CostQuoteProgram
 --                              From dayandr.ReeferTLD TLD
--                               left join t_self_service_nba_na.stratpro_tenders T on T.loadnum = TLD.LoadNum
--                               --Where 
--                                --             lower(trim(T.quotetypedesc)) <> 'salt'
--                               GROUP BY
--                                T.Loadnum
--                ;

                                
Drop table if  exists dayandr.ReeferMaxBuy;
  
create table dayandr.ReeferMaxBuy    
as                                            
SELECT 
                lm.loadnum
                ,count(distinct(PL.maxcost)) as MaxBuyCount

                from dayandr.ReeferTLD lm 
                inner join dbreporting.bt_loadprofitlog pl on lm.loadnum = pl.loadnum
                where lower(trim(lm.`condition`)) not in ('i','u')
                group by 
                lm.loadnum 
                ;
                
Drop table if  exists dayandr.ReeferADC;
  
create table dayandr.ReeferADC            
as
SELECT 
                lm.loadnum
                ,count(ad.SeqNum) as RollCount
    ,datediff(Max(ad.NewActivityDate),min(ad.OldActivityDate)) as DaysRolled
                                
                from dayandr.ReeferTLD lm 
                inner join dbreporting.bt_activitydatechange ad on lm.loadnum = ad.loadnum
                where lower(trim(lm.`condition`)) not in ('i','u')
                
                group by 
                lm.loadnum

                ;

Drop table if  exists dayandr.ReeferOffers;
  
create table dayandr.ReeferOffers        
as
SELECT
                LM.loadnum
                ,lm.MaxBuy
                ,case when O.OfferPrice>lm.MaxBuy then 1
                                else 0 end as OffersOverMax
                ,case when O.OfferPrice<lm.MaxBuy then 1
                                else 0 end as OffersUnderMax
                ,case when o.conditionalcode is not null then 1
                                        else 0 end as ConditionalOffers
                ,case when o.OfferPrice-lm.MaxBuy >0 then o.OfferPrice-lm.MaxBuy
                                else 0 end as OverMax
                ,case when o.OfferPrice-lm.MaxBuy <0 then o.OfferPrice-lm.MaxBuy
                                else 0 end as UnderMax
                ,O.OfferID
                                                                
                From dayandr.ReeferTLD lm 
                inner join edl.express_rap_dbo_offers as o on lm.loadnum = o.loadnum
                where lower(trim(lm.`condition`)) not in ('i','u')
                
                --group by 
                --lm.loadnum
               -- ,lm.MaxBuy
               -- ,o.OfferPrice
                --,O.OfferID
                ;
                
  
Drop table if  exists dayandr.ReeferAgOffer;
create table dayandr.ReeferAgOffer     
                as
SELECT
                O.LoadNum
                ,Sum(O.offersovermax)  as OffersOverMax
                ,Count(O.OfferID) -Sum(O.offersovermax) as OffersUnderMax
                ,Count(O.OfferID) as OfferCount
                ,Sum(o.ConditionalOffers) as ConditionalOffers
                ,AVG(O.OverMax) as AvgOverMax
                ,AVG(O.UnderMax) as AvgUnderMax
                From dayandr.ReeferOffers O
                group by 
                O.LoadNum
                ;
                
--Drop table if  exists bangbob.ReeferFlatOffers;
--  
--create table bangbob.ReeferFlatOffers 
--                as
--SELECT
--A.LoadNum
--,Sum(A.offersovermax) as OffersOverMax
--,Sum(A.Offersundermax)  as OffersUnderMax
--,Sum(A.ConditionalOffers) as ConditionalOffers
--,Sum(A.Offercount) as OfferCount
--,Sum(A.AvgOverMax) as AvgOverMax
--,Sum(A.AvgUnderMax) as AvgUnderMax
--
--from bangbob.ReeferAgOffer A
--group by A.LoadNum
--;

Drop table if  exists t_self_service_nba.TLReefer;

create table t_self_service_nba.TLReefer  
as
Select Distinct
TLD.Loadnum,
        --TLD.Mode as Mode,
        TLD.ModeNameSimple,
        TLD.entereddatetime,
        TLD.entereddate,
        TLD.activateddatetime,
        TLD.ActivityDate,
        TLD.bookdatetime,
        TLD.bookdate,
       -- TLD.PUAppt,
       -- TLD.ActivationLeadTime,
--        TLD.pickclosedatetime,
--        TLD.pickclosedate,
        --TLD.DELAppt,
       -- TLD.DelArrivalDate,
--        TLD.completeddatetime,
--        TLD.completeddate,
--        TLD.canceldatetime,
--        TLD.canceldate,
--        TLD.ActiveTimeToCancelMintues,
--        TLD.ActiveTimeToCancel,
--        TLD.EnteredTimeToCancelMintues,
--        TLD.EnteredTimeToCancel,
        TLD.ActiveTimeToBookMinutes,
        TLD.ActiveTimeToBook,
        TLD.CarrierLeadTimeDays,
        TLD.CarrierLeadTime,
        TLD.CustomerLeadTimeDays,
        TLD.CustomerLeadTime,
       -- TLD.DeliveryDate,
--        TLD.LoadActivationStatus,
        TLD.Rating,
        TLD.RateLevel,
        TLD.TeamFlag,
        TLD.HazMat,
        TLD.OverDimensionalYN,
        TLD.isHeavyHaul,
        TLD.Value,
        TLD.HVFlag,
        --TLD.MinTemp,
        --TLD.MaxTemp,
        TLD.StopCount,
        TLD.StopQuantity,
        TLD.`condition` as loadcondition,
        TLD.LoadType,
        --TLD.MaxStatus,
        TLD.CarrierReps,
        TLD.Books,
        TLD.CarrierBranch,
        TLD.CarrierCode,
        TLD.CarrierName,
        TLD.CarrierProgram,
        TLD.RespBranch as custrepbranch,
        TLD.Bounces,
        TLD.Consol,
        TLD.LoadBookingOutcome,
        TLD.BillToCompCode,
        TLD.CustomerBranch,
        TLD.ControlRepBranch,
        TLD.CustomerRep,
        TLD.ControlRep,
        TLD.CarrierRep,
        TLD.BookedBy,
        TLD.Rate,
        TLD.LinehaulRate,
        TLD.FuelRate,
        TLD.TotalAccessorialRate,
        TLD.LHPlusFuelNormalizedRate,
        TLD.AllInNormalizedRate,
        TLD.Cost,
        TLD.LinehaulCost,
        TLD.FuelCost,
        TLD.TotalAccessorialCost,
        TLD.LHPlusFuelNormalizedCost,
        TLD.AllInNormalizedCost,
        TLD.doefuelchrmiles,
        --TLD.PreHoldbackSpread,
        --TLD.HoldBackRev,
        TLD.Spread,
        --TLD.CustomerSpread,
        --TLD.CarrierSpread,
        --TLD.ControlSpread,
        --TLD.NegLoadYN,
        TLD.Miles,
        TLD.DropTrailerFlagPick,
        TLD.DropTrailerFlagDrop,
        TLD.DropTrailerPickDropType,
--        TLD.PickSchedule,
--                                TLD.DropSchedule,
--                                TLD.OriginLoadWork,
--                                TLD.DestUnloadWork,
--                                TLD.OriginOpenTime,
--                                TLD.OriginCloseTime,
--                                TLD.DestOpenTime,
--                                TLD.DestCloseTime,
        TLD.OriginWarehouseCode,
        TLD.OriginWarehouseName,
        TLD.DestinationWarehouseCode,
        TLD.DestinationWarehouseName,
        TLD.OZip,
        TLD.DZip,
        TLD.OZip3Digit,
        TLD.DZip3Digit,
        --TLD.OZip2Digit,
        --TLD.DZip2Digit,
        TLD.OriginCity,
        TLD.OriginState,
        TLD.OriginCityState,
        TLD.OriginStateCountry,
        TLD.OriginCountry,
        TLD.DestinationCity,
        TLD.DestinationState,
        TLD.DestinationCityState,
        TLD.DestinationStateCountry,
        TLD.DestinationCountry,
        TLD.Lane,
        TLD.`od state match yn` as odstatematch,
        --TLD.`total cross covered`,
--        TLD.primarybusinessline,
--        TLD.primarybusinesslineid,
        TLD.TMCLoad,
     TLD.EAR2Name,
--                                TLD.EAR2ID,
--                                TLD.NRTier,
--                                TLD.DATOrigin,
--                                TLD.DATCitySTOrig,
--                                TLD.DATDest,
--                                TLD.DATCitySTDest,
--                                TLD.DATCorridor,
--                                TLD.DATCorridorCS,
--                                TLD.NonLocalLOH,
--                                TLD.LocalLOH,
                                TLD.CostGroup,
                                TLD.BookingRule,
                                TLD.isregulatedbystf,
                                TLD.relationship,
                                TLD.isawarded,
                                TLD.maxbuy,
                                TLD.OverMaxBuy,
                                TLD.AmtOverMax,
                                TLD.UnderMaxBuy,
                                TLD.AmtUnderMax,
--                                TLD.ModeClean,
--                                TLD.totaloffers,
                                TLD.offersaccepted,
                                TLD.profitableoffers,
                                TLD.ismultileg,
--                                TLD.sic4name, 
--                                TLD.IndustrySegment,
--                                TLD.SizeSegment,
                                FO.OffersOverMax
                ,FO.OffersUnderMax
                ,FO.OfferCount
                ,FO.AvgOverMax
                ,FO.AvgUnderMax
                ,FO.ConditionalOffers
                ,MB.MaxBuyCount
                ,ADC.RollCount
    ,ADC.DaysRolled
--    ,CQ.CostQuoteProgram
    ,TLD.web
    --,b.Bounces
    ,b.SameDayBounces


From dayandr.ReeferTLD TLD
--left outer join dayandr.ReeferCostQuote CQ on TLD.loadnum = CQ.loadnum
left outer join dayandr.reeferagoffer FO on TLD.loadnum = FO.loadnum
left outer join dayandr.ReeferMaxBuy MB on TLD.loadnum = MB.loadnum
left outer join dayandr.ReeferADC ADC on TLD.loadnum = ADC.loadnum
left outer join dayandr.ReeferBounce B on b.loadnum = tld.loadnum 
;

Drop TABLE if exists dayandr.ReeferTLD;
Drop Table if exists dayandr.ReeferBounce;
--Drop table if  exists dayandr.ReeferCostQuote;
Drop table if  exists dayandr.ReeferADC;
Drop table if  exists dayandr.ReeferOffers;
Drop table if  exists dayandr.ReeferAgOffer;
Drop table if  exists dayandr.ReeferFlatOffers;
