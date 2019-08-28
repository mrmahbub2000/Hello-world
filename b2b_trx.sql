select a.part_date,a.cnt1,b.cnt2
from    (   select part_date,count(bsns_trx_id) cnt1
            from temp_rez11 
            group by part_date ) a
left outer join
        (   select part_date,count(bsns_trx_id) cnt2
            from EXTERNAL.DWB_AG_B2B_TRX
            group by part_date ) b
on a.part_date = b.part_date   
order by 1         

grant select on temp_rez12 to public

create table temp_rez12 as
SELECT A.BSNS_TRX_ID TRX_ID,
       TO_CHAR(A.TRX_TIMESTAMP, 'YYYY-MM-DD') TRX_DATE,
       TO_CHAR(A.TRX_TIMESTAMP, 'HH24:MI') TRX_TIME,
       A.TRX_IND TRX_TYPE, 
       A.AG_WLT,
       A.RA_WLT,
       A.MA_WLT,
       B.REGION_NAME, 
       RSM, 
       RSM_EMAIL
FROM (SELECT * FROM ad_hoc.temp_rez11--EXTERNAL.DWB_AG_B2B_TRX 
            WHERE PART_DATE >= '01-sep-15'--to_date(:P11139_D1,'DD-MON-YYYY') 
            and PART_DATE < '09-sep-15'--to_date(:P11139_D1,'DD-MON-YYYY') + 1
        ) A 
LEFT JOIN (SELECT MA_WLT, REGION_NAME, RSM, RSM_EMAIL FROM MFSDM_SYS.DWD_REGION WHERE VERSION=2) B
ON A.MA_WLT=B.MA_WLT


from temp_rez11  
where part_date >= '01-sep-15' and part_date < '09-sep-15'
order by 1

select * from EXTERNAL.DWB_AG_B2B_TRX where part_date = '01-sep-15'

--truncate table temp_rez11
Declare
v_dt date := to_date('31-aug-15','dd-mon-yy') ;
Begin
--create table temp_rez11 as
execute immediate 'truncate table temp_rez11' ;
For rec in 1..16 Loop
v_dt := v_dt + 1 ;
                insert into temp_rez11
                SELECT DISTINCT bsns_trx.day_key day_key, bsns_trx.bsns_trx_id bsns_trx_id,
                                bsns_trx.trx_timestamp trx_timestamp,
                                bsns_trx.trx_type_code trx_type_code, 'B2B RECEIVE' trx_ind,
                                bsns_trx.src_wlt ag_wlt, bsns_trx.dst_wlt ra_wlt,
                                channel_tree.ma_wlt ma_wlt, bsns_trx.trx_amt trx_amt,
                                bsns_trx.trx_channel trx_channel,
                                bsns_trx.part_date part_date
                           FROM foundation.dwb_bsns_trx bsns_trx,
                                mfsdm_sys.dwl_channel_tree channel_tree,
                                mfsdm_sys.dwl_bsns_kyc bsns_kyc
                          WHERE (1 = 1)
                            AND (bsns_trx.part_date = v_dt --'14-sep-15' 
                                                        )--TRUNC (SYSDATE - 1))
                            AND (bsns_trx.trx_type_code = 5001)
                            --AND (channel_tree.part_date(+) >= TRUNC (SYSDATE - 1))
                            AND (channel_tree.part_date(+) >= v_dt --'14-sep-15' 
                            and channel_tree.part_date(+) < v_dt+1 --'15-sep-15' 
                                )
                            AND (bsns_trx.dst_wlt = channel_tree.ra_wlt(+))
                            AND (    bsns_trx.src_wlt = bsns_kyc.wallet_number
                                 AND bsns_kyc.type_name IN
                                              ('PSEUDO AGENT', 'AGENT', 'DISCONTINUED AGENT')
                                )
                UNION
                SELECT DISTINCT bsns_trx.day_key day_key, bsns_trx.bsns_trx_id bsns_trx_id,
                                bsns_trx.trx_timestamp trx_timestamp,
                                bsns_trx.trx_type_code trx_type_code, 'B2B SEND' trx_ind,
                                bsns_trx.dst_wlt ag_wlt, bsns_trx.src_wlt ra_wlt,
                                channel_tree.ma_wlt ma_wlt,   (bsns_trx.trx_amt)
                                                            * (-1) trx_amt,
                                bsns_trx.trx_channel trx_channel,
                                bsns_trx.part_date part_date
                           FROM foundation.dwb_bsns_trx bsns_trx,
                                mfsdm_sys.dwl_channel_tree channel_tree,
                                mfsdm_sys.dwl_bsns_kyc bsns_kyc
                          WHERE (1 = 1)
                            --AND (channel_tree.part_date(+) >= TRUNC (SYSDATE - 1))
                            AND (channel_tree.part_date(+) >= v_dt --'14-sep-15' 
                            and channel_tree.part_date(+) < v_dt+1 --'15-sep-15' 
                            )
                            AND (bsns_trx.trx_type_code = 5001)
                            AND (bsns_trx.part_date = v_dt --'14-sep-15'
                                                        )
                                                --TRUNC (SYSDATE - 1))
                            AND (bsns_trx.src_wlt = channel_tree.ra_wlt(+))
                            AND (    bsns_trx.dst_wlt = bsns_kyc.wallet_number
                                 AND bsns_kyc.type_name IN
                                              ('PSEUDO AGENT', 'AGENT', 'DISCONTINUED AGENT')
                                ) ;
                Commit ;
End loop;                
End ;                