create or replace package Resources_Load is
  /*
  --*******************************************************************
    -- Author  : PRIYANKA.JAIN
    -- Created : 4/17/2017 10:48:09
    -- Author  : BHARTI BHATIA
    -- Updates :6/1/2017 - till date
    -- Purpose : To load data into resource/capacitiy and ODS tables
    -- This package was initially written for Sort and Test resources and later extended to include
    -- Assembly/BUMP/Diecoat,Fab and Non ATE resources
  --********************************************************************
  */

  -- Public function and procedure declarations

  --Below procedure loading data from SNP to SRC table
  Procedure load_RES_PART_ASN_SRC(i_enterprise_nm IN VARCHAR2);
  Procedure load_RES_ATTR_SRC; --(i_enterprise_nm IN VARCHAR2);
  Procedure load_RES_CAP_SRC(i_enterprise_nm IN VARCHAR2);
  Procedure load_RES_HW_ALT_SRC(i_enterprise_nm IN VARCHAR2);
  Procedure load_RES_FAB_ASN_SRC(i_enterprise_nm IN VARCHAR2); ----to populate FAB resources Added by VS 10/17
  PROCEDURE load_asmbly_cap(i_enterprise_nm IN VARCHAR2); -- to poulate Assembly capacity
  Procedure load_RES_NONATE_ATTR_SRC(i_enterprise_nm IN VARCHAR2); ----to populate NON ATE ATTR_SRC Added by VS 11/01
  PROCEDURE load_Assy_Part_Vnd_Src(i_enterprise_nm IN VARCHAR2); ---- to populate Assembly/Bump/Diecoat resources
  PROCEDURE load_Assy_Vnd_Alloc_Src(i_enterprise_nm IN VARCHAR2); ---- to populate srcs for proportional vendor split
  PROCEDURE load_bod_dtl_tp_src(i_enterprise_nm IN VARCHAR2);

  --Below procedure load data from SRC tables  to ODS tables
  PROCEDURE load_RESOURCEMASTER(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_RESOURCECALENDAR(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_WORKCENTERMASTER(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_WORKCENTERDETAIL(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_CALENDARMASTER(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_CALENDARDETAIL(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_OPRESOURCE(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_OPRESOURCEADD(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_OPRESOURCEALT(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_RES_RTE_ASN_SRC(i_enterprise_nm IN VARCHAR2);
  PROCEDURE UPD_RES_RTE_ASN_SRC_FLG(i_enterprise_nm IN VARCHAR2);
  PROCEDURE LOAD_RES_RTE_ASN_DTLNN_SRC(i_enterprise_nm IN VARCHAR2);
  PROCEDURE LOAD_RES_RTE_ASN_DTL_SRC(i_enterprise_nm IN VARCHAR2);
  PROCEDURE LOAD_ROUTE_RES_ALT_DTLRTE_SRC(i_enterprise_nm IN VARCHAR2);
  PROCEDURE load_RES_RTE_ALT_SRC(i_enterprise_nm IN VARCHAR2);
  PROCEDURE LOAD_ROUTINGHEADER(i_enterprise_nm IN VARCHAR2);
  PROCEDURE LOAD_ROUTINGOPERATION(i_enterprise_nm IN VARCHAR2);
  PROCEDURE LOAD_ITEMBOMROUTING(i_enterprise_nm IN VARCHAR2);
  PROCEDURE LOAD_OPCALENDAR_STG(i_enterprise_nm IN VARCHAR2);

-- PROCEDURE LOAD_OPRESOURCE_ALTRTE(i_enterprise_nm IN VARCHAR2);
-- PROCEDURE LOAD_OPRESOURCEADD_ALTRTE(i_enterprise_nm IN VARCHAR2);

end Resources_Load;
/
CREATE OR REPLACE PACKAGE BODY Resources_Load IS

  --Variable declaration
  lv_row_source VARCHAR2(100) := NULL;
  -- li_enterprise_nm ref_data.eng_enterprise_ref.enterprise%TYPE := 'SCP_DAILY';
  -- lv_cur_date   DATE := NULL;
  lv_rpt_dt DATE := NULL;

  l_curr_dt    date := dates.get_rpt_dt;
  l_horizon_dt date := dates.get_rpt_dt(i_rpt_nm => 'HORIZON_END');

  -- Function and procedure implementations
  --************************************************************
  --TO load data into part_res_asn_src table from SNP source
  -- This procedure is used to load data for Sort and Test resources
  --************************************************************
  PROCEDURE load_RES_PART_ASN_SRC(i_enterprise_nm IN VARCHAR2) IS
  
  BEGIN
  
    frmdata.logs.begin_log('Start RES_PART_ASN_SRC load from SNP to SRC');
    frmdata.delete_data('RES_PART_ASN_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    -- lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date'23-May-2017';
  
    lv_row_source := 'P001';
    ----Below logics handles for both ATE and NON ATE
    INSERT INTO scmdata.res_part_asn_src
      (ENTERPRISE,
       SITE_NUM,
       MFG_PART_NUM,
       PROD_RTE_SEQ_ID,
       SAP_RTE_ID,
       RTE_SEQ_NUM,
       ENG_RTE_ID,
       ENG_OPER_SEQ,
       STEP_NM,
       QUAL_CD,
       RES_AREA,
       RTE_PRIO_CD,
       EFF_START_DT,
       EFF_END_DT,
       PRDN_FLG,
       PRIM_RES_SET_ID,
       ETL_DT,
       SOURCE_DT,
       ROW_SOURCE)
      SELECT eer.enterprise, --hardcoded enterprise value  --removed hardcoding
             prr.SITE_NUM,
             prr.MFG_PART_NUM,
             prr.PROD_RTE_SEQ_ID,
             prr.SAP_RTE_ID,
             prr.OPER_SEQ,
             rts.ENG_RTE_ID,
             rts.ENG_OPER_SEQ,
             prr.STEP_NM,
             prr.QUAL_CD,
             prr.RES_AREA,
             prr.RTE_PRIO_CD,
             prr.EFF_START_DT,
             prr.EFF_END_DT,
             prr.PRDN_FLG,
             prr.PRIM_RES_SET_ID,
             prr.ETL_DT,
             sysdate,
             'RPA-01'
        FROM scmdata.PROD_RES_RELATION_SNP_VW prr,
             scmdata.Rte_Src                  rts,
             ref_data.eng_enterprise_ref      eer
       WHERE rts.mfg_part_num = prr.mfg_part_num
         AND rts.sap_rte_id = prr.sap_rte_id
         AND eer.enterprise = i_enterprise_nm
         AND eer.ent_res_flg like '%:' || prr.RES_AREA || ':%' --added by SA on 12/22
         AND rts.oper_seq = prr.OPER_SEQ;
  
    COMMIT; /* remove later  */
    frmdata.logs.info('RES_PART_ASN_SRC load from SNP to SRC is completed');
  
  END load_res_part_asn_src;

  --************************************************************
  -- This procedure is used to load data for Sort and Test resources
  --************************************************************
  PROCEDURE load_RES_ATTR_SRC IS
    --(i_enterprise_nm IN VARCHAR2) IS
  
    --variable declaration
  
    lv_hw_type           scmdata.HARDWARE_SET_SNP_VW.HW_TYPE%TYPE;
    lv_hw_name           scmdata.HARDWARE_SET_SNP_VW.HW_NM%TYPE;
    lv_resource_nm       bill_of_resources_snp_vw.res_nm%TYPE := NULL;
    lv_PROD_route_Seq_ID res_alternates_snp_vw.prod_rte_seq_id%TYPE;
    lv_prim_res_set_id   res_alternates_snp_vw.prim_res_set_id%TYPE; --new column
    lv_RESOURCE_SET_ID   res_alternates_snp_vw.res_set_id%TYPE;
    lv_RES_PRIORITY_CD   res_alternates_snp_vw.prio_cd%TYPE;
    lv_RESOURCE_TYPE     bill_of_resources_snp_vw.res_type%TYPE;
    lv_QTY_REQUIRED      bill_of_resources_snp_vw.required_qty%TYPE;
    lv_UTPH              resource_attr_snp_vw.attr_val_num%TYPE;
    lv_TESTER_GROUP      scmdata.resource_attr_snp_vw.attr_val_char%TYPE;
    lv_Sprint_UTPH       scmdata.resource_attr_snp_vw.attr_val_num%TYPE;
    lv_etl_dt            scmdata.res_alternates_snp_vw.etl_dt%TYPE;
    ---------------------------------------------------------------------------
    CURSOR cur_res_attr IS
      SELECT ral.PROD_rte_Seq_ID,
             ral.PRIM_RES_SET_ID, --new column
             ral.RES_SET_ID,
             ral.PRIO_CD,
             bor.RES_TYPE,
             bor.RES_NM,
             bor.REQUIRED_QTY,
             ral.etl_dt,
             (SELECT rat.attr_val_num
                FROM scmdata.resource_attr_snp_vw rat
               WHERE rat.attr_nm = 'UTPH'
                 AND rat.attr_set_id = ral.attr_set_id) UTPH,
             (SELECT rat.attr_val_char
                FROM scmdata.resource_attr_snp_vw rat
               WHERE rat.attr_nm = 'TESTER_GROUP'
                 AND rat.attr_set_id = ral.attr_set_id) TESTER_GROUP,
             (SELECT rat.attr_val_num
                FROM scmdata.resource_attr_snp_vw rat
               WHERE rat.attr_nm = 'SPRINT_UTPH'
                 AND rat.attr_set_id = ral.attr_set_id) SPRINT_UTPH
        FROM scmdata.Res_Alternates_Snp_vw    ral,
             frmdata.bill_of_resources_snp_vw bor
      -- scmdata.bill_of_resources_snp_vw bor
       WHERE ral.res_set_id = bor.res_set_id
      --AND ral.attr_set_id=3004
       order by 1;
  
    curval_res_attr cur_res_attr%ROWTYPE;
    ----------------------------------------------------------------
    --cursor to get hardware information
    ----------------------------------------------------------------
    CURSOR cur_hardware_set IS
      SELECT hss.HW_SET_ID, hss.HW_TYPE, hss.hw_nm, hss.required_qty
        FROM scmdata.hardware_set_snp_vw hss
       WHERE hss.hw_set_id = lv_resource_nm --4001
      ;
    curval_hardware cur_hardware_set%ROWTYPE;
    ------------------------------------------------------------------
    --Initializing variable
    ------------------------------------------------------------------
    PROCEDURE init_var IS
    BEGIN
      lv_row_source        := 'A001';
      lv_hw_name           := NULL;
      lv_hw_type           := NULL;
      lv_resource_nm       := NULL;
      lv_PROD_route_Seq_ID := NULL;
      lv_prim_res_set_id   := NULL; --new column
      lv_RESOURCE_SET_ID   := NULL;
      lv_RES_PRIORITY_CD   := NULL;
      lv_RESOURCE_TYPE     := NULL;
      lv_QTY_REQUIRED      := NULL;
      lv_UTPH              := NULL;
      lv_TESTER_Group      := NULL;
      lv_Sprint_UTPH       := NULL;
      lv_etl_dt            := NULL;
    END;
    -------------------------------------------------------------------
    -- procedure to insert value into table
    -------------------------------------------------------------------
    PROCEDURE insert_into_table IS
    BEGIN
    
      INSERT INTO RES_ATTR_SRC
        ( --ENTERPRISE,
         PROD_RTE_SEQ_ID,
         PRIM_RES_SET_ID, --new column
         RES_SET_ID,
         RES_PRIO,
         RES_TYPE,
         RES_NM,
         REQUIRED_QTY,
         UPH,
         RES_GRP,
         Sprint_UPH,
         HW_TYPE,
         HW_NM,
         ETL_DT,
         SOURCE_DT,
         ROW_SOURCE)
      VALUES
        ( --i_enterprise_nm,
         lv_PROD_route_Seq_ID,
         lv_prim_res_set_id, --new column
         lv_RESOURCE_SET_ID,
         lv_RES_PRIORITY_CD,
         lv_RESOURCE_TYPE,
         lv_RESOURCE_NM,
         lv_QTY_REQUIRED,
         lv_UTPH,
         lv_TESTER_GROUP,
         lv_SPRINT_UTPH,
         lv_hw_type,
         lv_hw_name,
         lv_ETL_DT,
         sysdate,
         'RAS-01');
    END;
    ---------------------------------------------------------------------
  BEGIN
  
    frmdata.logs.begin_log('Start load_RES_ATTR_SRC load from SNP to SRC');
    frmdata.delete_data('RES_ATTR_SRC', NULL, NULL);
  
    --  lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date
  
    FOR curval_res_attr IN cur_res_attr LOOP
      init_var; --initiallize variable
    
      lv_PROD_route_Seq_ID := curval_res_attr.prod_rte_seq_id;
      lv_prim_res_set_id   := curval_res_attr.PRIM_RES_SET_ID; --new column
      lv_RESOURCE_SET_ID   := curval_res_attr.res_set_id;
      lv_RES_PRIORITY_CD   := curval_res_attr.prio_cd;
      lv_RESOURCE_TYPE     := curval_res_attr.res_type;
      lv_QTY_REQUIRED      := curval_res_attr.required_qty;
      lv_UTPH              := curval_res_attr.utph;
      lv_Tester_Group      := curval_res_attr.tester_group;
      lv_Sprint_UTPH       := curval_res_attr.sprint_utph;
      lv_etl_dt            := curval_res_attr.etl_dt;
      lv_resource_nm       := curval_res_attr.res_nm;
    
      IF (curval_res_attr.Res_Type = 'HARDWARE_SET' AND -----changed from HW_SET_ID to HARDWARE_SET added by VS 12/20/17
         lv_resource_nm IS NOT NULL) THEN
        lv_row_source := lv_row_source || '|H001';
      
        FOR curval_hardware IN cur_hardware_set LOOP
          lv_hw_type      := curval_hardware.HW_type;
          lv_hw_name      := curval_hardware.hw_nm;
          lv_QTY_REQUIRED := curval_hardware.required_qty; --override resource qty with hw qty
        
          --insert value into table for hardware only when hardware name is not null
          IF lv_hw_name IS NOT NULL THEN
            insert_into_table;
          END IF;
        
        END LOOP;
      ELSE
        lv_hw_type := NULL;
        lv_hw_name := NULL;
      
        insert_into_table;
      
      END IF;
    
    END LOOP;
    COMMIT; /* remove later  */
    frmdata.logs.info('RES_ATTR_SRC load from SNP to SRC is completed');
  END load_RES_ATTR_SRC;

  --************************************************************
  --Procedure to load data into RES_CAP_SRC table
  --************************************************************
  Procedure load_RES_CAP_SRC(i_enterprise_nm IN VARCHAR2) IS
  
    -- This cursor is used to load data for Sort and Test resources
    -- The cursor manipulates data for resource_nm's that exceed 40 chars
  
    CURSOR cur_res_capacity_mp IS
      SELECT DISTINCT dms.fy,
                      dms.qtr_num,
                      dms.wk_num,
                      dms.wk_bgn_dt,
                      dms.wk_end_dt,
                      rca.res_nm,
                      trim(mtab.mapresource_nm) || '_' || ldr.list_val_char ||
                      rca.site_num engine_res_nm,
                      rca.site_num,
                      case
                        WHEN dms.wk_bgn_dt >
                             (select distinct wk_bgn_dt
                                from scmdata.DMCLS_SNP_VW dms
                               where wk_num =
                                     (select wk_num + 26
                                        from scmdata.DMCLS_SNP_VW dms
                                       where cal_dt =
                                             scmdata.engines.get_plan_currentdate(i_enterprise_nm))
                                 AND FY =
                                     (select Fy
                                        from scmdata.DMCLS_SNP_VW dms
                                       where cal_dt =
                                             scmdata.engines.get_plan_currentdate(i_enterprise_nm))) THEN
                         100000
                        ELSE
                         ceil(greatest(rca.res_cnt, rca.eff_res_cnt))
                      END RES_CNT,
                      CASE
                        WHEN dms.wk_bgn_dt >
                             (select distinct wk_bgn_dt
                                from scmdata.DMCLS_SNP_VW dms
                               where wk_num =
                                     (select wk_num + 26
                                        from scmdata.DMCLS_SNP_VW dms
                                       where cal_dt =
                                             scmdata.engines.get_plan_currentdate(i_enterprise_nm))
                                 AND FY =
                                     (select Fy
                                        from scmdata.DMCLS_SNP_VW dms
                                       where cal_dt =
                                             scmdata.engines.get_plan_currentdate(i_enterprise_nm))) THEN
                         100000
                        ELSE
                         rca.eff_res_cnt
                      END EFF_RES_CNT,
                      rca.RES_TYPE,
                      rca.res_area,
                      rca.ETL_DT,
                      rca.eff_start_dt,
                      rca.eff_end_dt
        FROM scmdata.Resource_Capacity_Snp_vw rca,
             ref_data.site_master_ref smr,
             (SELECT RES_NM,
                     SUBSTR(RES_NM, 1, 24),
                     ROW_NUMBER() OVER(PARTITION BY SUBSTR(RES_NM, 1, 24) ORDER BY RES_NM) rnk,
                     SUBSTR(RES_NM, 1, 24)
                     
                     || '_' || ROW_NUMBER() OVER(PARTITION BY SUBSTR(RES_NM, 1, 24) ORDER BY RES_NM) mapresource_nm
                FROM (SELECT DISTINCT RES_NM --,site_id
                        FROM scmdata.Resource_Capacity_Snp_vw rca,
                             ref_data.site_master_ref         smr
                       WHERE rca.site_num = smr.site_num
                         AND smr.plannable_flg = 'Y'
                         AND LENGTH(rca.res_nm) > 24
                      --and resource_nm like 'SP21/MAX16814/QFND4X4%' --'AJ24/MAX4886/TQFN9X3.5%'
                      )) mtab,
             scmdata.DMCLS_SNP_VW dms,
             ref_data.list_dtl_ref ldr,
             ref_data.eng_enterprise_ref eer --addedd by SA on 12/22
       WHERE rca.site_num = smr.site_num
         AND eer.enterprise = i_enterprise_nm --addedd by SA on 12/22
         AND eer.ent_res_flg like '%:' || rca.RES_AREA || ':%' --addedd by SA on 12/22
         AND smr.plannable_flg = 'Y'
         AND ldr.list_dtl_num = rca.res_area
         AND dms.WK_BGN_DT >= rca.eff_start_dt
         AND dms.WK_END_DT <= l_horizon_dt --rca.eff_end_dt--Added to limit res_cnt till horizon_end
         and rca.res_area not like 'NON ATE FT'
         AND LENGTH(rca.res_nm) > 24
         AND mtab.RES_NM = rca.res_nm
       order by dms.fy, dms.qtr_num, dms.wk_num, rca.res_nm, rca.Site_num;
  
    -- This cursor is used to load data for Sort and Test resources
  
    CURSOR cur_res_capacity IS
      SELECT /*  +first_rows */
      DISTINCT dms.fy,
               dms.qtr_num,
               dms.wk_num,
               dms.wk_bgn_dt,
               dms.wk_end_dt,
               rca.res_nm,
               rca.Site_num,
               trim(rca.res_nm) || '_' || ldr.list_val_char || rca.site_num engine_res_nm,
               --  rca.res_cnt,
               case --WHEN ( dms.wk_num >= 37 and to_number(dms.fy) >= 2017 )
                 WHEN dms.wk_bgn_dt >
                      (select distinct wk_bgn_dt
                         from scmdata.DMCLS_SNP_VW dms
                        where wk_num =
                              (select wk_num + 26
                                 from scmdata.DMCLS_SNP_VW dms
                                where cal_dt =
                                      scmdata.engines.get_plan_currentdate(i_enterprise_nm))
                          AND FY =
                              (select Fy
                                 from scmdata.DMCLS_SNP_VW dms
                                where cal_dt =
                                      scmdata.engines.get_plan_currentdate(i_enterprise_nm))) THEN
                  100000
                 ELSE
                  ceil(greatest(rca.res_cnt, rca.eff_res_cnt))
               END RES_CNT,
               CASE
                 WHEN dms.wk_bgn_dt >
                      (select distinct wk_bgn_dt
                         from scmdata.DMCLS_SNP_VW dms
                        where wk_num =
                              (select wk_num + 26
                                 from scmdata.DMCLS_SNP_VW dms
                                where cal_dt =
                                      scmdata.engines.get_plan_currentdate(i_enterprise_nm))
                          AND FY =
                              (select Fy
                                 from scmdata.DMCLS_SNP_VW dms
                                where cal_dt =
                                      scmdata.engines.get_plan_currentdate(i_enterprise_nm))) THEN
                  100000
                 ELSE
                  rca.eff_res_cnt
               END EFF_RES_CNT,
               rca.res_type,
               rca.res_area,
               rca.ETL_DT,
               rca.eff_start_dt,
               rca.eff_end_dt
        FROM scmdata.Resource_Capacity_Snp_vw rca,
             ref_data.site_master_ref         smr,
             scmdata.DMCLS_SNP_VW             dms,
             ref_data.list_dtl_ref            ldr,
             ref_data.eng_enterprise_ref      eer --addedd by SA on 12/22
       WHERE rca.site_num = smr.site_num
         AND eer.enterprise = i_enterprise_nm --addedd by SA on 12/22
         AND eer.ent_res_flg like '%:' || rca.RES_AREA || ':%' --addedd by SA on 12/22
         AND smr.plannable_flg = 'Y'
         AND ldr.list_dtl_num = rca.res_area
         and rca.res_area not like 'NON ATE FT'
         AND dms.WK_BGN_DT >= rca.eff_start_dt
         AND dms.WK_END_DT <= l_horizon_dt --rca.eff_end_dt--Added to limit res_cnt till horizon_end
         AND LENGTH(rca.res_nm) <= 24
       order by dms.fy, dms.qtr_num, dms.wk_num, rca.res_nm, rca.Site_num;
  
    curval_res_capacity_mp cur_res_capacity_mp%ROWTYPE;
    curval_res_capacity    cur_res_capacity%ROWTYPE;
  
    ------Below logic is for Fab Resources-----
    --created an iterm table for fab_proc data with all 65 weeks data.
    procedure insert_fab(p_res_nm in varchar2, p_fab_num in varchar2) is
    
    begin
      delete from fab_proc_cap_interm
       where res_nm = p_res_nm
         and fab_num = p_fab_num;
      commit;
    
      INSERT INTO fab_proc_cap_interm
        SELECT nvl(a.res_nm, p_res_nm) res_nm,
               nvl(a.fab_num, p_fab_num) fab_num,
               b.yr_num,
               b.wk_num,
               CASE
                 WHEN b.wk_bgn_dt < LAST_VALUE(a.wk_bgn_dt IGNORE NULLS)
                  OVER(ORDER BY fab_num) AND a.wk_wfr_cp IS NULL THEN
                  0
                 WHEN b.wk_bgn_dt > LAST_VALUE(a.wk_bgn_dt IGNORE NULLS)
                  OVER(ORDER BY fab_num) AND a.wk_wfr_cp IS NULL THEN
                  LAST_VALUE(a.wk_wfr_cp IGNORE NULLS)
                  OVER(ORDER BY fab_num)
                 ELSE
                  a.wk_wfr_cp
               END wk_wfr_cp,
               NVL(a.dt, sysdate) dt,
               b.qtr_num
          from (SELECT nvl(fpc.res_nm, p_res_nm) res_nm,
                       nvl(fpc.fab_num, p_fab_num) fab_num,
                       cal.yr_num,
                       cal.wk_num,
                       fpc.wk_wfr_cap wk_wfr_cp,
                       fpc.updt_dt dt,
                       cal.qtr_num,
                       cal.wk_bgn_dt
                  FROM (SELECT DISTINCT wk_num, yr_num, qtr_num, wk_bgn_dt
                          FROM dmcls_snp_vw
                         WHERE cal_dt BETWEEN l_curr_dt AND l_horizon_dt
                         order by yr_num, wk_num) cal,
                       fab_process_cap_snp fpc
                 WHERE cal.wk_num = fpc.wk_num
                   and cal.yr_num = fpc.yr_num
                   and fpc.res_nm = p_res_nm
                   and fpc.fab_num = p_fab_num
                 order by fpc.fab_num, cal.yr_num, cal.wk_num) a,
               (SELECT DISTINCT wk_num, yr_num, qtr_num, wk_bgn_dt
                  FROM dmcls_snp_vw
                 WHERE cal_dt BETWEEN l_curr_dt AND l_horizon_dt
                 order by yr_num, wk_num) b
         where a.wk_num(+) = b.wk_num
           AND a.YR_NUM(+) = B.YR_NUM
           AND a.qtr_num(+) = b.qtr_num
           AND a.wk_bgn_dt(+) = b.wk_bgn_dt
         ORDER BY a.res_nm, b.yr_num, b.wk_num;
    
    end insert_fab;
  
  BEGIN
  
    frmdata.logs.begin_log('Start RES_CAP_SRC load from SNP to SRC');
    --  frmdata.delete_data('RES_CAP_SRC', NULL, NULL);
    frmdata.delete_data('RES_CAP_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    -- Loading data for Fab resources
  
    IF i_enterprise_nm = 'SCP_STARTS' ------------Added by VS 10/17
     THEN
      lv_row_source := 'F001';
    
      for i in (select distinct res_nm, fab_num from fab_process_cap_snp_vw) loop
        insert_fab(i.res_nm, i.fab_num);
      end loop;
    
      INSERT INTO RES_CAP_SRC
        (enterprise,
         res_nm,
         eng_res_nm,
         site_num,
         wk_num,
         yr_num,
         qtr_num,
         res_cnt,
         eff_res_cnt,
         res_type,
         res_area,
         etl_dt,
         source_dt,
         row_source,
         vnd_num,
         day_of_wk,
         cap_uom,
         bkt_uom)
        SELECT DISTINCT eer.enterprise,
                        fpc.res_nm,
                        fpc.res_nm || '_' || smr.eng_site_num,
                        fpc.fab_num,
                        fpc.wk_num,
                        fpc.yr_num,
                        cal.qtr_num,
                        fpc.wk_wfr_cap,
                        fpc.wk_wfr_cap,
                        'FAB' res_type,
                        'FAB' res_area,
                        fpc.updt_dt,
                        SYSDATE source_dt,
                        'RCS-01',
                        NULL vnd_num,
                        NULL day_of_wk,
                        'WAFER' cap_uom,
                        'WEEK' bkt_uom
          FROM ref_data.eng_enterprise_ref eer,
               ref_data.site_master_ref    smr,
               dmcls_snp_vw                cal,
               fab_proc_cap_interm         fpc
         WHERE eer.enterprise = i_enterprise_nm
           AND eer.ent_res_flg like '%:' || 'FAB' || ':%' --addedd by SA on 12/22
           AND smr.site_num = fpc.fab_num
           AND smr.plannable_flg = 'Y'
           AND cal.wk_num = fpc.wk_num
           AND cal.yr_num = fpc.yr_num
           and cal.qtr_num = fpc.qtr_num;
    
    ELSE
    
      --Cursor to get all the site and resources with capacities
      -- Loading data for Sort and test resources
      FOR curval_res_capacity IN cur_res_capacity LOOP
      
        lv_row_source := 'C001';
      
        INSERT INTO RES_CAP_SRC
          (enterprise,
           res_nm,
           eng_res_nm,
           site_num,
           wk_num,
           yr_num,
           qtr_num,
           res_cnt,
           eff_res_cnt,
           res_type,
           res_area,
           etl_dt,
           source_dt,
           row_source)
        --mapped_res_name
        /*      INSERT INTO RES_CAP_SRC */
        VALUES
          (i_enterprise_nm,
           curval_res_capacity.res_nm,
           curval_res_capacity.engine_res_nm,
           curval_res_capacity.site_num,
           curval_res_capacity.wk_num,
           curval_res_capacity.fy,
           curval_res_capacity.qtr_num,
           curval_res_capacity.res_cnt,
           curval_res_capacity.eff_res_cnt,
           curval_res_capacity.res_type,
           curval_res_capacity.res_area,
           curval_res_capacity.etl_dt,
           sysdate,
           'RCS-02');
      
      --    NULL;
      
      END LOOP;
    
      FOR curval_res_capacity_mp IN cur_res_capacity_mp LOOP
      
        lv_row_source := 'D001';
      
        INSERT INTO RES_CAP_SRC
          (enterprise,
           res_nm,
           eng_res_nm,
           site_num,
           wk_num,
           yr_num,
           qtr_num,
           res_cnt,
           eff_res_cnt,
           res_type,
           res_area,
           etl_dt,
           source_dt,
           row_source)
        --mapped_res_name
        /*      INSERT INTO RES_CAP_SRC */
        VALUES
          (i_enterprise_nm,
           curval_res_capacity_mp.res_nm,
           curval_res_capacity_mp.engine_res_nm,
           curval_res_capacity_mp.site_num,
           curval_res_capacity_mp.wk_num,
           curval_res_capacity_mp.fy,
           curval_res_capacity_mp.qtr_num,
           curval_res_capacity_mp.res_cnt,
           curval_res_capacity_mp.eff_res_cnt,
           curval_res_capacity_mp.res_type,
           curval_res_capacity_mp.res_area,
           curval_res_capacity_mp.etl_dt,
           sysdate,
           'RCS-03');
      
      --    NULL;
      
      END LOOP;
      ---Loading data into RES_CAP_SRC for NON ATE Resources
      INSERT INTO res_cap_src
        (enterprise,
         res_nm,
         eng_res_nm,
         site_num,
         wk_num,
         yr_num,
         qtr_num,
         res_cnt,
         eff_res_cnt,
         res_type,
         res_area,
         etl_dt,
         source_dt,
         row_source,
         vnd_num,
         day_of_wk,
         cap_uom,
         bkt_uom)
        SELECT DISTINCT eer.enterprise,
                        rcs.res_nm,
                        rcs.res_nm || '_' || smr.eng_site_num,
                        rcs.site_num,
                        rcs.wk_num,
                        rcs.yr_num,
                        rcs.qtr_num,
                        res_cnt,
                        res_cnt eff_res_snt,
                        rcs.res_type,
                        rcs.res_area,
                        rcs.etl_dt,
                        SYSDATE source_dt,
                        'RCS-04',
                        NULL vnd_num,
                        NULL day_of_wk,
                        NULL cap_uom,
                        NULL bkt_uom
          FROM ref_data.eng_enterprise_ref eer,
               ref_data.site_master_ref smr,
               (select distinct temp.res_nm,
                                temp.site_num,
                                max(temp.new_res_cnt) over(partition by temp.res_nm, temp.site_num, dm.yr_num, dm.wk_num order by temp.res_nm, temp.site_num, dm.yr_num, dm.wk_num) res_cnt,
                                TEMP.res_area,
                                TEMP.res_type,
                                TEMP.etl_dt,
                                dm.wk_num,
                                dm.yr_num,
                                dm.qtr_num
                  from (SELECT dt.START_DT,
                               dt.END_DT,
                               dt.RES_NM,
                               dt.SITE_NUM,
                               CASE
                                 WHEN dt.start_dt = rcs.eff_start_dt AND
                                      dt.END_DT = rcs.EFF_END_DT THEN
                                  rcs.res_cnt
                                 WHEN dt.end_dt = l_horizon_dt THEN
                                  dt.res_cnt
                                 ELSE
                                  0
                               END new_res_cnt,
                               DT.res_area,
                               DT.res_type,
                               DT.etl_dt
                          FROM (SELECT START_DT,
                                       NVL(LEAD(START_DT)
                                           OVER(PARTITION BY RES_NM,
                                                SITE_NUM ORDER BY RES_NM,
                                                SITE_NUM,
                                                START_DT),
                                           l_horizon_dt) END_DT,
                                       RES_NM,
                                       SITE_NUM,
                                       res_cnt,
                                       res_area,
                                       res_type,
                                       etl_dt
                                  FROM (SELECT START_DT,
                                               RES_NM,
                                               SITE_NUM,
                                               res_cnt,
                                               res_area,
                                               res_type,
                                               etl_dt
                                          FROM ((SELECT RES_NM,
                                                        SITE_NUM,
                                                        EFF_START_DT,
                                                        EFF_END_DT,
                                                        res_cnt,
                                                        res_area,
                                                        res_type,
                                                        etl_dt
                                                   FROM RESOURCE_CAPACITY_SNP_VW a
                                                  WHERE RES_AREA LIKE 'NON%')
                                                UNPIVOT(START_DT FOR
                                                        VALUE_TYPE IN
                                                        (EFF_START_DT,
                                                         EFF_END_DT))))) dt,
                               resource_capacity_snp_vw rcs
                         WHERE dt.START_DT != dt.END_DT
                           AND dt.res_nm = rcs.res_nm(+)
                           AND dt.site_num = rcs.site_num(+)
                           AND dt.START_DT = rcs.EFF_START_DT(+)
                           AND dt.END_DT = rcs.EFF_END_DT(+)
                           and rownum > 0) temp,
                       dmcls_snp_vw dm
                 where dm.cal_dt BETWEEN temp.start_dt AND temp.END_dt - 1
                   AND dm.cal_dt BETWEEN l_curr_dt AND l_horizon_dt
                   and rownum > 0
                 ORDER BY RES_NM, YR_NUM, WK_NUM) rcs
         WHERE eer.enterprise = i_enterprise_nm
           AND eer.ent_res_flg like '%:' || rcs.RES_AREA || ':%' --added by SA on 12/22
           AND rcs.site_num = smr.site_num
           AND smr.plannable_flg = 'Y'
         order by RES_NM, YR_NUM, WK_NUM;
    
    END IF;
    Commit; /* remove later  */
    frmdata.logs.info('RES_CAP_SRC load from SNP to SRC is completed');
  
  END load_RES_CAP_SRC;

  --************************************************************
  --Procedure to load data into RES_CAP_SRC table for Asmbly resources
  --************************************************************
  PROCEDURE LOAD_ASMBLY_CAP(i_enterprise_nm IN VARCHAR2) IS
  
    ln_cnt_rows INTEGER;
  
    lv_resource_cd   assy_daily_cap_details_snp.res_cd%TYPE;
    lv_resource_nm   assy_daily_cap_details_snp.res_nm%TYPE;
    lv_vendor_id     assy_daily_cap_details_snp.vnd_num%TYPE;
    ln_start_cap_qty INTEGER;
    ln_cap_qty       INTEGER;
    ld_savestart_dt  DATE;
    ln_counter       INTEGER := 0;
    ld_planstart_dt  DATE;
    ld_planend_dt    DATE;
    ln_halt_cond     NUMBER(1);
  
    -- changing res_area = 'PKG' for 'PKG'  instead of ' Assembly' to keep it consistent bb 1/3/2018
  
    /*
     This cursor is used to identify resources that have eff_start_dt both in the past and the future
    */
  
    CURSOR cur_res_capacity_gt IS
      select m1.*
        from (select distinct dtl.res_cd,
                              dtl.res_nm,
                              dtl.vnd_num,
                              mstr.halt_flg,
                              mstr.halt_end_dt
                from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                     ASSY_DAILY_CAP_MASTER_SNP  mstr,
                     ref_data.site_master_ref   smr
               where dtl.res_nm = mstr.res_nm
                 and dtl.res_cd = mstr.res_cd
                 and dtl.vnd_num = mstr.vnd_num
                 and mstr.plannable_flg = 'Y'
                 and dtl.vnd_num = smr.
               site_num -- ltrim(smr.site_num, '0')
                 and smr.plannable_flg = 'Y'
                 and dtl.eff_start_dt > ld_planstart_dt) m1,
             (select distinct dtl1.res_nm, dtl1.vnd_num
                from assy_daily_cap_details_snp dtl1,
                     assy_daily_cap_master_snp  mstr1,
                     ref_data.site_master_ref   smr1
               where dtl1.res_nm = mstr1.res_nm
                 and dtl1.res_cd = mstr1.res_cd
                 and dtl1.vnd_num = mstr1.vnd_num
                 and mstr1.plannable_flg = 'Y'
                 and dtl1.vnd_num = smr1.
               site_num -- ltrim(smr.site_num, '0')
                 and smr1.plannable_flg = 'Y'
                 and dtl1.eff_start_dt < ld_planstart_dt) m2
       where m1.res_nm = m2.res_nm
         and m1.vnd_num = m2.vnd_num;
  
    -- details of the distinct resources to process
    CURSOR cur_res_capacity_gtres IS
      select dtl.res_cd,
             dtl.res_nm,
             dtl.VND_NUM,
             dtl.vnd_nm,
             dtl.eff_start_dt,
             dtl.daily_cap_qty,
             mstr.cap_uom AS CAP_UOM,
             smr.site_num,
             smr.eng_site_num,
             'DAILY' as Bkt_UOM,
             mstr.halt_flg,
             mstr.halt_end_dt
        from ASSY_DAILY_CAP_DETAILS_SNP dtl,
             ASSY_DAILY_CAP_MASTER_SNP  mstr,
             ref_data.site_master_ref   smr
       where dtl.res_nm = mstr.res_nm
         and dtl.res_cd = mstr.res_cd
         and dtl.VND_NUM = mstr.VND_NUM
         and mstr.PLANNABLE_FLG = 'Y'
         and dtl.VND_NUM = smr.
       site_num -- ltrim(smr.site_num, '0')
         and smr.plannable_flg = 'Y'
         and dtl.res_cd = lv_resource_cd
         and dtl.res_nm = lv_resource_nm
         and dtl.VND_NUM = lv_vendor_id
         and dtl.eff_start_dt > ld_planstart_dt
       order by dtl.eff_start_dt;
  
    -- curval_res_capacity_gtexc cur_res_capacity_gtexc%ROWTYPE;
    curval_res_capacity_gt    cur_res_capacity_gt%ROWTYPE;
    curval_res_capacity_gtres cur_res_capacity_gtres%ROWTYPE;
  
    PROCEDURE LOAD_ASMBLY_CAP_EXC AS
    
      ln_cnt_rowsexc INTEGER;
    
      lv_resource_cdexc assy_daily_cap_details_snp.res_cd%type;
      lv_resource_nmexc assy_daily_cap_details_snp.res_nm%type;
      lv_vendor_idexc   assy_daily_cap_details_snp.vnd_num%type;
      --  ln_start_cap_qty INTEGER;
      ln_cap_qtyexc      INTEGER;
      ld_savestart_dtexc DATE;
      ln_counter_exc     INTEGER := 0;
      ld_planstart_dtexc DATE;
      ld_planend_dtexc   DATE;
      -- ln_halt_condexc    NUMBER(1);
    
      /*
        This procedure is used to identify resources that have no data in the past but only eff_start_dt in the future
        compared to SCMDATA.Dates.get_rpt_dt
      
      */
    
      CURSOR cur_res_capacity_gtexc IS
        select m1.*
          from (select distinct dtl.res_cd,
                                dtl.res_nm,
                                dtl.VND_NUM,
                                mstr.halt_flg,
                                mstr.halt_end_dt
                  from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                       ASSY_DAILY_CAP_MASTER_SNP  mstr,
                       ref_data.site_master_ref   smr
                -- where resource_nm = '8LTQFN'
                 where dtl.res_nm = mstr.res_nm
                   and dtl.res_cd = mstr.res_cd
                   and dtl.vnd_num = mstr.vnd_num
                   and mstr.plannable_flg = 'Y'
                   and dtl.vnd_num = smr.
                 site_num -- ltrim(smr.site_num, '0')
                   and smr.plannable_flg = 'Y'
                   and dtl.eff_start_dt > ld_planstart_dt) m1
         where not exists (select 1
                  from -- and dtl.res_nm,dtl.VND_NUM  in
                       (select distinct dtl1.res_cd,
                                        dtl1.res_nm,
                                        dtl1.VND_NUM
                          from ASSY_DAILY_CAP_DETAILS_SNP dtl1,
                               ASSY_DAILY_CAP_MASTER_SNP  mstr1,
                               ref_data.site_master_ref   smr1
                         where dtl1.res_nm = mstr1.res_nm
                           and dtl1.res_cd = mstr1.res_cd
                           and dtl1.vnd_num = mstr1.vnd_num
                           and mstr1.plannable_flg = 'Y'
                           and dtl1.vnd_num = smr1.
                         site_num -- ltrim(smr.site_num, '0')
                           and smr1.plannable_flg = 'Y'
                           and dtl1.eff_start_dt < ld_planstart_dt) m2
                 where m1.res_cd = m2.res_cd
                   and m1.res_nm = m2.res_nm
                   and m1.vnd_num = m2.vnd_num);
    
      CURSOR cur_res_capacity_gtresexc IS
        select dtl.res_cd,
               dtl.res_nm,
               dtl.VND_NUM,
               dtl.VND_NM,
               dtl.eff_start_dt,
               dtl.daily_cap_qty,
               mstr.cap_uom AS CAP_UOM,
               smr.site_num,
               smr.eng_site_num,
               'DAILY' as Bkt_UOM,
               mstr.halt_flg,
               mstr.halt_end_dt
          from ASSY_DAILY_CAP_DETAILS_SNP dtl,
               ASSY_DAILY_CAP_MASTER_SNP  mstr,
               ref_data.site_master_ref   smr
         where dtl.res_nm = mstr.res_nm
           and dtl.res_cd = mstr.res_cd
           and dtl.vnd_num = mstr.vnd_num
           and mstr.plannable_flg = 'Y'
           and dtl.vnd_num = smr.
         site_num -- ltrim(smr.site_num, '0')
           and smr.plannable_flg = 'Y'
           and dtl.res_cd = lv_resource_cdexc
           and dtl.res_nm = lv_resource_nmexc
           and dtl.vnd_num = lv_vendor_idexc
           and dtl.eff_start_dt > ld_planstart_dt
         order by dtl.eff_start_dt;
    
      curval_res_capacity_gtexc    cur_res_capacity_gtexc%ROWTYPE;
      curval_res_capacity_gtresexc cur_res_capacity_gtresexc%ROWTYPE;
    
    BEGIN
    
      ld_planstart_dtexc := SCMDATA.Dates.get_rpt_dt;
      ld_planend_dtexc   := SCMDATA.Dates.get_rpt_dt('HORIZON_END');
    
      FOR curval_res_capacity_gtexc IN cur_res_capacity_gtexc LOOP
      
        /*  dbms_output.put_line('exc res is ' ||
        curval_res_capacity_gtexc.res_nm ||
        ' vend is  ' ||
        curval_res_capacity_gtexc.VND_NUM);*/
        --  dbms_output.put_line('vend is' || curval_res_capacity_gt.VND_NUM);
      
        lv_resource_cdexc := curval_res_capacity_gtexc.res_cd;
        lv_resource_nmexc := curval_res_capacity_gtexc.res_nm;
        lv_vendor_idexc   := curval_res_capacity_gtexc.VND_NUM;
        ln_counter_exc    := 0;
      
        select count(*)
          into ln_cnt_rowsexc
          from (select *
                  from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                       ASSY_DAILY_CAP_MASTER_SNP  mstr,
                       ref_data.site_master_ref   smr
                 where dtl.res_nm = mstr.res_nm
                   and dtl.res_cd = mstr.res_cd
                   and dtl.VND_NUM = mstr.VND_NUM
                   and mstr.PLANNABLE_FLG = 'Y'
                   and dtl.VND_NUM = smr.
                 site_num -- ltrim(smr.site_num, '0')
                   and smr.plannable_flg = 'Y'
                   and dtl.res_cd = curval_res_capacity_gtexc.res_cd
                   and dtl.res_nm = curval_res_capacity_gtexc.res_nm
                   and dtl.VND_NUM = curval_res_capacity_gtexc.VND_NUM
                   and dtl.eff_start_dt > ld_planstart_dtexc
                 order by dtl.eff_start_dt);
      
        FOR curval_res_capacity_gtresexc IN cur_res_capacity_gtresexc LOOP
        
          ln_counter_exc := ln_counter_exc + 1;
        
          --     dbms_output.put_line('ln_counter is  ' || ln_counter);
        
          /*   IF (curval_res_capacity_gtresexc.HALT_FLG = 'Y' AND
             curval_res_capacity_gtresexc.HALT_END_DT <
             curval_res_capacity_gtresexc.eff_start_dt) THEN
          
            exit;
          
          END IF;*/
        
          --   IF curval_res_capacity_gt.halt_end_dt <= ld_planstart_dt THEN
        
          IF curval_res_capacity_gt.halt_end_dt > ld_planend_dt THEN
          
            dbms_output.put_line('No planning for this resource ' ||
                                 curval_res_capacity_gt.res_nm);
            exit;
            -- exit for this resource_cd,VND_NUM no capacity planning
            --null;
          
          END IF;
        
          IF ln_counter_exc = 1 THEN
          
            ld_savestart_dtexc := curval_res_capacity_gtresexc.eff_start_dt;
            ln_cap_qtyexc      := curval_res_capacity_gtresexc.daily_cap_qty;
          
          END IF;
        
          IF ln_counter_exc != ln_cnt_rowsexc THEN
          
            INSERT INTO res_cap_src
              (res_nm,
               eng_res_nm,
               site_num,
               res_area,
               res_type,
               --res_cnt,
               eff_res_cnt,
               cap_uom,
               bkt_uom,
               day_of_wk,
               wk_num,
               qtr_num,
               yr_num,
               vnd_num,
               enterprise,
               source_dt,
               row_source)
              SELECT rc.res_nm,
                     rc.eng_res_nm,
                     rc.site_num,
                     rc.RES_AREA,
                     rc.RES_TYPE,
                     rc.EFF_RES_CNT,
                     rc.CAP_UOM,
                     rc.Bkt_UOM,
                     rc.WORK_DAY,
                     rc.Work_WEEK,
                     rc.QUARTER,
                     rc.YEAR,
                     rc.VND_NUM,
                     eer.enterprise,
                     sysdate,
                     'RCS-05'
                FROM (SELECT assmbly.RES_NM AS RES_NM,
                             assmbly.RES_NM || '_' || assmbly.eng_site_num AS ENG_RES_NM,
                             assmbly.eng_site_num AS SITE_NUM,
                             assmbly.res_cd AS RES_AREA,
                             assmbly.res_cd AS RES_TYPE,
                             assmbly.daily_cap_qty AS EFF_RES_CNT,
                             assmbly.cap_uom AS CAP_UOM,
                             'DAILY' as Bkt_UOM,
                             v.day_of_wk AS WORK_DAY,
                             v.wk_num AS Work_WEEK,
                             v.qtr_num AS QUARTER,
                             v.yr_num AS YEAR,
                             assmbly.VND_NUM
                      -- i_enterprise_nm,
                        FROM scmdata.DMCLS_SNP_VW v,
                             (select dtl.res_cd,
                                     dtl.res_nm,
                                     dtl.VND_NUM,
                                     dtl.vnd_nm,
                                     dtl.eff_start_dt,
                                     ln_cap_qtyexc daily_cap_qty,
                                     mstr.cap_uom AS CAP_UOM,
                                     smr.site_num,
                                     smr.eng_site_num,
                                     'DAILY' as Bkt_UOM,
                                     mstr.halt_flg,
                                     mstr.halt_end_dt
                                from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                                     ASSY_DAILY_CAP_MASTER_SNP  mstr,
                                     ref_data.site_master_ref   smr
                               where dtl.res_nm = mstr.res_nm
                                 and dtl.res_cd = mstr.res_cd
                                 and dtl.VND_NUM = mstr.VND_NUM
                                 and mstr.PLANNABLE_FLG = 'Y'
                                 and dtl.VND_NUM = smr.
                               site_num -- ltrim(smr.site_num, '0')
                                 and smr.plannable_flg = 'Y'
                                 and dtl.res_cd =
                                     curval_res_capacity_gtexc.res_cd
                                 and dtl.res_nm =
                                     curval_res_capacity_gtexc.res_nm
                                 and dtl.VND_NUM =
                                     curval_res_capacity_gtresexc.VND_NUM
                                 and rownum < 2) assmbly
                      /* where v.cal_dt >= ld_savestart_dtexc
                      and v.cal_dt <= (CASE
                            WHEN (curval_res_capacity_gtresexc.HALT_FLG = 'Y' AND
                                 curval_res_capacity_gtresexc.HALT_END_DT <
                                 curval_res_capacity_gtresexc.eff_start_dt) THEN
                             curval_res_capacity_gtresexc.HALT_END_DT - 1
                            ELSE
                             curval_res_capacity_gtresexc.eff_start_dt
                          END)*/
                       where v.cal_dt >= (CASE
                               WHEN curval_res_capacity_gtresexc.HALT_FLG = 'Y' THEN
                                greatest(curval_res_capacity_gtresexc.HALT_END_DT,
                                         ld_savestart_dtexc)
                               ELSE
                                ld_savestart_dtexc
                             END)
                         and v.cal_dt <=
                             curval_res_capacity_gtresexc.eff_start_dt -- modified bb 1/10/2018 to accomadate halt_dt change
                      
                      ) rc,
                     eng_enterprise_ref_snp_vw eer --added by SA on 12/22
               WHERE eer.enterprise = i_enterprise_nm
                 AND eer.ent_res_flg like '%:' || rc.RES_AREA || ':%';
          
            ld_savestart_dtexc := curval_res_capacity_gtres.eff_start_dt;
            ln_cap_qtyexc      := curval_res_capacity_gtres.daily_cap_qty;
          
          ELSE
          
            INSERT INTO res_cap_src
              (res_nm,
               eng_res_nm,
               site_num,
               res_area,
               res_type,
               --res_cnt,
               eff_res_cnt,
               cap_uom,
               bkt_uom,
               day_of_wk,
               wk_num,
               qtr_num,
               yr_num,
               vnd_num,
               enterprise,
               source_dt,
               row_source)
              SELECT rc.res_nm,
                     rc.eng_res_nm,
                     rc.site_num,
                     rc.RES_AREA,
                     rc.RES_TYPE,
                     rc.EFF_RES_CNT,
                     rc.CAP_UOM,
                     rc.Bkt_UOM,
                     rc.WORK_DAY,
                     rc.Work_WEEK,
                     rc.QUARTER,
                     rc.YEAR,
                     rc.VND_NUM,
                     eer.enterprise,
                     sysdate,
                     'RCS-06'
                FROM (SELECT assmbly.res_nm AS RES_NM,
                             assmbly.res_nm || '_' || assmbly.eng_site_num AS ENG_RES_NM,
                             assmbly.eng_site_num AS SITE_NUM,
                             assmbly.res_cd AS RES_AREA,
                             assmbly.res_cd AS RES_TYPE,
                             assmbly.daily_cap_qty AS EFF_RES_CNT,
                             assmbly.cap_uom AS CAP_UOM,
                             'DAILY' as Bkt_UOM,
                             v.day_of_wk AS WORK_DAY,
                             v.wk_num AS Work_WEEK,
                             v.qtr_num AS QUARTER,
                             v.yr_num AS YEAR,
                             assmbly.VND_NUM
                        FROM scmdata.DMCLS_SNP_VW v,
                             (select dtl.res_cd,
                                     dtl.res_nm,
                                     dtl.VND_NUM,
                                     dtl.vnd_nm,
                                     dtl.eff_start_dt,
                                     ln_cap_qtyexc daily_cap_qty,
                                     mstr.cap_uom AS CAP_UOM,
                                     smr.site_num,
                                     smr.eng_site_num,
                                     'DAILY' as Bkt_UOM
                              -- select resource_nm,VND_NUM,max(eff_start_dt) -- 1028 rows
                                from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                                     ASSY_DAILY_CAP_MASTER_SNP  mstr,
                                     ref_data.site_master_ref   smr
                               where dtl.res_nm = mstr.res_nm
                                 and dtl.res_cd = mstr.res_cd
                                 and dtl.VND_NUM = mstr.VND_NUM
                                 and mstr.PLANNABLE_FLG = 'Y'
                                 and dtl.VND_NUM = smr.
                               site_num -- ltrim(smr.site_num, '0')
                                 and smr.plannable_flg = 'Y'
                                 and dtl.res_cd =
                                     curval_res_capacity_gtexc.res_cd
                                 and dtl.res_nm =
                                     curval_res_capacity_gtexc.res_nm
                                 and dtl.vnd_num =
                                     curval_res_capacity_gtresexc.vnd_num
                                 and rownum < 2) assmbly
                      /* where v.cal_dt >= ld_savestart_dtexc
                      and v.cal_dt <= (CASE
                            WHEN (curval_res_capacity_gtresexc.HALT_FLG = 'Y' AND
                                 curval_res_capacity_gtresexc.HALT_END_DT <
                                 ld_planend_dt) THEN
                             curval_res_capacity_gtresexc.HALT_END_DT - 1
                            ELSE
                             ld_planend_dtexc
                          END)*/
                       where v.cal_dt >= (CASE
                               WHEN curval_res_capacity_gtresexc.HALT_FLG = 'Y' THEN
                                greatest(curval_res_capacity_gtresexc.HALT_END_DT,
                                         ld_savestart_dtexc)
                               ELSE
                                ld_savestart_dtexc
                             END)
                         and v.cal_dt <= ld_planend_dtexc) rc, -- modified bb 1/10/2018 to accomadate halt_dt change
                     eng_enterprise_ref_snp_vw eer --added by SA on 12/22
               WHERE eer.enterprise = i_enterprise_nm
                 AND eer.ent_res_flg like '%:' || rc.RES_AREA || ':%';
          
          END IF;
        
        END LOOP;
      
      END LOOP;
    
    END; -- END PROC  LOAD_ASMBLY_CAP_EXC Defn
  
  BEGIN
  
    ld_planstart_dt := SCMDATA.Dates.get_rpt_dt;
    ld_planend_dt   := SCMDATA.Dates.get_rpt_dt('HORIZON_END');
  
    /*
    Code to do direct inserts for resources that only have eff_start_dt in the past
    */
  
    INSERT INTO res_cap_src
      (res_nm,
       eng_res_nm,
       site_num,
       res_area,
       res_type,
       --res_cnt,
       eff_res_cnt,
       cap_uom,
       bkt_uom,
       day_of_wk,
       wk_num,
       qtr_num,
       yr_num,
       vnd_num,
       enterprise,
       source_dt,
       row_source)
      SELECT rc.res_nm,
             rc.eng_res_nm,
             rc.site_num,
             rc.RES_AREA,
             rc.RES_TYPE,
             rc.EFF_RES_CNT,
             rc.CAP_UOM,
             rc.Bkt_UOM,
             rc.WORK_DAY,
             rc.Work_WEEK,
             rc.QUARTER,
             rc.YEAR,
             rc.VND_NUM,
             eer.enterprise,
             sysdate,
             'RCS-07'
        FROM (SELECT assmbly.res_nm AS RES_NM,
                     assmbly.res_nm || '_' || assmbly.eng_site_num AS ENG_RES_NM,
                     assmbly.eng_site_num AS SITE_NUM,
                     assmbly.res_cd AS RES_AREA,
                     assmbly.res_cd AS RES_TYPE,
                     -- null ,
                     assmbly.daily_cap_qty AS EFF_RES_CNT,
                     assmbly.cap_uom AS CAP_UOM,
                     'DAILY' as Bkt_UOM,
                     v.day_of_wk AS WORK_DAY,
                     v.wk_num AS Work_WEEK,
                     v.qtr_num AS QUARTER,
                     v.yr_num AS YEAR,
                     assmbly.VND_NUM
                FROM scmdata.DMCLS_SNP_VW v,
                     (SELECT *
                        FROM (SELECT res_cd,
                                     res_nm,
                                     VND_NUM,
                                     vnd_nm,
                                     eff_start_dt,
                                     daily_cap_qty,
                                     cap_uom,
                                     bkt_uom,
                                     site_num,
                                     eng_site_num,
                                     halt_flg,
                                     halt_end_dt,
                                     dense_rank() over(partition by res_nm, VND_NUM order by eff_start_dt desc) as dnsrnk
                                from (select dtl.res_cd,
                                             dtl.res_nm,
                                             dtl.VND_NUM,
                                             dtl.vnd_nm,
                                             dtl.eff_start_dt,
                                             dtl.daily_cap_qty,
                                             mstr.cap_uom AS CAP_UOM,
                                             smr.site_num,
                                             smr.eng_site_num,
                                             'DAILY' as Bkt_UOM,
                                             mstr.halt_flg,
                                             mstr.halt_end_dt
                                        from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                                             ASSY_DAILY_CAP_MASTER_SNP  mstr,
                                             ref_data.site_master_ref   smr
                                       where dtl.res_nm = mstr.res_nm
                                         and dtl.res_cd = mstr.res_cd
                                         and dtl.VND_NUM = mstr.VND_NUM
                                         and mstr.PLANNABLE_FLG = 'Y'
                                         and dtl.VND_NUM = smr.
                                       site_num -- ltrim(smr.site_num, '0')
                                         and smr.plannable_flg = 'Y'))
                       WHERE DNSRNK = 1
                         AND EFF_START_DT < ld_planstart_dt) assmbly
               where v.cal_dt >= (CASE
                       WHEN assmbly.HALT_FLG = 'Y' AND
                            assmbly.HALT_END_DT < ld_planstart_dt THEN
                        ld_planstart_dt
                       WHEN assmbly.HALT_FLG = 'Y' AND
                            assmbly.HALT_END_DT between ld_planstart_dt and
                            ld_planend_dt THEN
                        HALT_END_DT
                       WHEN assmbly.HALT_FLG = 'Y' AND
                            assmbly.HALT_END_DT > ld_planend_dt THEN
                        ld_planend_dt + 1
                       ELSE
                        ld_planstart_dt
                     END)
                 and v.cal_dt <= (ld_planend_dt)) rc, -- modified bb 1/10/2018 to accomadate halt_dt change
             --        where v.cal_dt >= ld_planstart_dt
             --      and v.cal_dt <= (CASE
             --           WHEN assmbly.HALT_FLG = 'Y' THEN
             --           assmbly.HALT_END_DT - 1
             --          ELSE
             --           ld_planend_dt
             --        END)) rc,
             eng_enterprise_ref_snp_vw eer --added by SA on 12/22
       WHERE eer.enterprise = i_enterprise_nm
         AND eer.ent_res_flg like '%:' || rc.RES_AREA || ':%';
  
    commit;
    LOAD_ASMBLY_CAP_EXC; -- execute procedure for exception resources
    commit;
  
    --Cursor to get all the site and resources with capacities regular rows
    FOR curval_res_capacity_gt IN cur_res_capacity_gt LOOP
    
      --  dbms_output.put_line('vend is' || curval_res_capacity_gt.VND_NUM);
    
      lv_resource_cd := curval_res_capacity_gt.res_cd;
      lv_resource_nm := curval_res_capacity_gt.res_nm;
      lv_vendor_id   := curval_res_capacity_gt.VND_NUM;
      ln_counter     := 0;
      ln_halt_cond   := 0;
    
      ld_savestart_dt := NULL;
      ln_cap_qty      := 0;
    
      select count(*)
        into ln_cnt_rows
        from (select *
                from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                     ASSY_DAILY_CAP_MASTER_SNP  mstr,
                     ref_data.site_master_ref   smr
               where dtl.res_nm = mstr.res_nm
                 and dtl.res_cd = mstr.res_cd
                 and dtl.VND_NUM = mstr.VND_NUM
                 and mstr.PLANNABLE_FLG = 'Y'
                 and dtl.VND_NUM = smr.site_num -- ltrim(smr.site_num, '0')
                 and smr.plannable_flg = 'Y'
                 and dtl.res_cd = curval_res_capacity_gt.res_cd
                 and dtl.res_nm = curval_res_capacity_gt.res_nm
                 and dtl.VND_NUM = curval_res_capacity_gt.VND_NUM
                 and dtl.eff_start_dt > ld_planstart_dt
               order by dtl.eff_start_dt);
    
      -- FOR curval_res_capacity_gtres IN cur_res_capacity_gtres LOOP
    
      --   ln_counter := ln_counter + 1;
    
      IF curval_res_capacity_gt.halt_flg = 'Y' THEN
      
        /* IF curval_res_capacity_gt.halt_end_dt < ld_planend_dt THEN
          ld_planend_dt := curval_res_capacity_gt.halt_end_dt;
          -- dbms_output.put_line('Halt flg is Y also halt end less than planenddate');
        END IF;*/
      
        --   IF curval_res_capacity_gt.halt_end_dt <= ld_planstart_dt THEN
      
        IF curval_res_capacity_gt.halt_end_dt > ld_planend_dt THEN
        
          /*   dbms_output.put_line('No planning for this resource ' ||
          curval_res_capacity_gt.res_nm);*/
          exit;
          -- exit for this resource_cd,VND_NUM no capacity planning
        ELSE
          -- continue
          --  dbms_output.put_line('Halt_flg is set and proceeding');
          FOR curval_res_capacity_gtres IN cur_res_capacity_gtres LOOP
            ln_counter := ln_counter + 1;
            --dbms_output.put_line('ln_counter is ' || ln_counter);
          
            IF cur_res_capacity_gtres%ROWCOUNT = 1 THEN
              --dbms_output.put_line('cur_res_capacity_gtres%ROWCOUNT IS ' ||
              --                     cur_res_capacity_gtres%ROWCOUNT);
            
              select dtl.daily_cap_qty rptdtqty
                INTO ln_start_cap_qty
                from ASSY_DAILY_CAP_DETAILS_SNP dtl
               where dtl.res_cd = curval_res_capacity_gtres.res_cd
                 and dtl.res_nm = curval_res_capacity_gt.res_nm
                 and dtl.VND_NUM = curval_res_capacity_gtres.vnd_num
                 and dtl.eff_start_dt =
                     (select max(dtl.eff_start_dt)
                        from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                             ASSY_DAILY_CAP_MASTER_SNP  mstr,
                             ref_data.site_master_ref   smr
                       where dtl.res_nm = mstr.res_nm
                         and dtl.res_cd = mstr.res_cd
                         and dtl.vnd_num = mstr.vnd_num
                         and mstr.PLANNABLE_FLG = 'Y'
                         and dtl.vnd_num = smr.site_num -- ltrim(smr.site_num, '0')
                         and smr.plannable_flg = 'Y'
                         and dtl.res_cd = curval_res_capacity_gtres.res_cd
                         and dtl.res_nm = curval_res_capacity_gt.res_nm
                         and dtl.vnd_num = curval_res_capacity_gtres.vnd_num
                         and dtl.eff_start_dt < ld_planstart_dt);
            
              INSERT INTO res_cap_src
                (res_nm,
                 eng_res_nm,
                 site_num,
                 res_area,
                 res_type,
                 --res_cnt,
                 eff_res_cnt,
                 cap_uom,
                 bkt_uom,
                 day_of_wk,
                 wk_num,
                 qtr_num,
                 yr_num,
                 vnd_num,
                 enterprise,
                 source_dt,
                 row_source)
                SELECT rc.res_nm,
                       rc.eng_res_nm,
                       rc.site_num,
                       rc.RES_AREA,
                       rc.RES_TYPE,
                       rc.EFF_RES_CNT,
                       rc.CAP_UOM,
                       rc.Bkt_UOM,
                       rc.WORK_DAY,
                       rc.Work_WEEK,
                       rc.QUARTER,
                       rc.YEAR,
                       rc.VND_NUM,
                       eer.enterprise,
                       sysdate,
                       'RCS-08'
                  FROM (SELECT assmbly.res_nm AS RES_NM,
                               assmbly.res_nm || '_' || assmbly.eng_site_num AS ENG_RES_NM,
                               assmbly.eng_site_num AS SITE_NUM,
                               assmbly.res_cd AS RES_AREA,
                               assmbly.res_cd AS RES_TYPE,
                               assmbly.daily_cap_qty AS EFF_RES_CNT,
                               assmbly.cap_uom AS CAP_UOM,
                               'DAILY' as Bkt_UOM,
                               v.day_of_wk AS WORK_DAY,
                               v.wk_num AS Work_WEEK,
                               v.qtr_num AS QUARTER,
                               v.yr_num AS YEAR,
                               assmbly.vnd_num
                          FROM scmdata.DMCLS_SNP_VW v,
                               (select dtl.res_cd,
                                       dtl.res_nm,
                                       dtl.vnd_num,
                                       dtl.vnd_nm,
                                       dtl.eff_start_dt,
                                       ln_start_cap_qty daily_cap_qty,
                                       mstr.cap_uom AS CAP_UOM,
                                       smr.site_num,
                                       smr.eng_site_num,
                                       'DAILY' as Bkt_UOM,
                                       mstr.halt_flg,
                                       mstr.halt_end_dt
                                  from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                                       ASSY_DAILY_CAP_MASTER_SNP  mstr,
                                       ref_data.site_master_ref   smr
                                 where dtl.res_nm = mstr.res_nm
                                   and dtl.res_cd = mstr.res_cd
                                   and dtl.vnd_num = mstr.vnd_num
                                   and mstr.PLANNABLE_FLG = 'Y'
                                   and dtl.vnd_num = smr.
                                 site_num -- ltrim(smr.site_num, '0')
                                   and smr.plannable_flg = 'Y'
                                   and dtl.res_cd =
                                       curval_res_capacity_gtres.res_cd
                                   and dtl.res_nm =
                                       curval_res_capacity_gt.res_nm
                                   and dtl.vnd_num =
                                       curval_res_capacity_gtres.vnd_num
                                   and rownum < 2) assmbly
                         where v.cal_dt >= (CASE
                                 WHEN assmbly.HALT_FLG = 'Y' AND
                                      assmbly.HALT_END_DT < ld_planstart_dt THEN
                                  ld_planstart_dt
                                 WHEN assmbly.HALT_FLG = 'Y' AND
                                      assmbly.HALT_END_DT between
                                      ld_planstart_dt and ld_planend_dt then
                                  HALT_END_DT
                                 WHEN assmbly.HALT_FLG = 'Y' AND
                                      assmbly.HALT_END_DT > ld_planend_dt THEN
                                  ld_planend_dt + 1
                                 ELSE
                                  ld_planstart_dt
                               END)
                           and v.cal_dt <
                               curval_res_capacity_gtres.eff_start_dt -- modified bb 1/10/2018 to accomadate halt_dt change
                        --to_date('4/6/2019', 'MM/DD/YYYY') --ld_planend_dt
                        ) rc,
                       eng_enterprise_ref_snp_vw eer --added by SA on 12/22
                 WHERE eer.enterprise = i_enterprise_nm
                   AND eer.ent_res_flg like '%:' || rc.RES_AREA || ':%';
            
              ld_savestart_dt := curval_res_capacity_gtres.eff_start_dt;
              ln_cap_qty      := curval_res_capacity_gtres.daily_cap_qty;
            
              --   END IF;
            
              /*   IF curval_res_capacity_gtres.halt_end_dt <
                 curval_res_capacity_gtres.eff_start_dt THEN
              
                ln_halt_cond := 1;
                -- dbms_output.put_line('ln_halt_cond_reached');
                exit;
              
              END IF;*/
              -- dbms_output.put_line('ld_savestart_dt is ' || ld_savestart_dt);
              -- dbms_output.put_line('ld_end_dt is ' ||
              --                      curval_res_capacity_gtres.eff_start_dt);
            
            ELSE
              -- from row 2 onwards in curval_res_capacity_gtres
            
              INSERT INTO res_cap_src
                (res_nm,
                 eng_res_nm,
                 site_num,
                 res_area,
                 res_type,
                 --res_cnt,
                 eff_res_cnt,
                 cap_uom,
                 bkt_uom,
                 day_of_wk,
                 wk_num,
                 qtr_num,
                 yr_num,
                 vnd_num,
                 enterprise,
                 source_dt,
                 row_source)
                SELECT rc.res_nm,
                       rc.eng_res_nm,
                       rc.site_num,
                       rc.RES_AREA,
                       rc.RES_TYPE,
                       rc.EFF_RES_CNT,
                       rc.CAP_UOM,
                       rc.Bkt_UOM,
                       rc.WORK_DAY,
                       rc.Work_WEEK,
                       rc.QUARTER,
                       rc.YEAR,
                       rc.VND_NUM,
                       eer.enterprise,
                       sysdate,
                       'RCS-09'
                  FROM (SELECT assmbly.res_nm AS RES_NM,
                               assmbly.res_nm || '_' || assmbly.eng_site_num AS ENG_RES_NM,
                               assmbly.eng_site_num AS SITE_NUM,
                               assmbly.res_cd AS RES_AREA,
                               assmbly.res_cd AS RES_TYPE,
                               assmbly.daily_cap_qty AS EFF_RES_CNT,
                               assmbly.cap_uom AS CAP_UOM,
                               'DAILY' as Bkt_UOM,
                               v.day_of_wk AS WORK_DAY,
                               v.wk_num AS Work_WEEK,
                               v.qtr_num AS QUARTER,
                               v.yr_num AS YEAR,
                               assmbly.vnd_num
                          FROM scmdata.DMCLS_SNP_VW v,
                               (select dtl.res_cd,
                                       dtl.res_nm,
                                       dtl.vnd_num,
                                       dtl.vnd_nm,
                                       dtl.eff_start_dt,
                                       ln_cap_qty daily_cap_qty,
                                       mstr.cap_uom AS CAP_UOM,
                                       smr.site_num,
                                       smr.eng_site_num,
                                       'DAILY' as Bkt_UOM
                                  from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                                       ASSY_DAILY_CAP_MASTER_SNP  mstr,
                                       ref_data.site_master_ref   smr
                                 where dtl.res_nm = mstr.res_nm
                                   and dtl.res_cd = mstr.res_cd
                                   and dtl.vnd_num = mstr.vnd_num
                                   and mstr.PLANNABLE_FLG = 'Y'
                                   and dtl.vnd_num = smr.
                                 site_num -- ltrim(smr.site_num, '0')
                                   and smr.plannable_flg = 'Y'
                                   and dtl.res_cd =
                                       curval_res_capacity_gtres.res_cd
                                   and dtl.res_nm =
                                       curval_res_capacity_gt.res_nm
                                   and dtl.vnd_num =
                                       curval_res_capacity_gtres.vnd_num
                                   and rownum < 2) assmbly
                         where v.cal_dt >= ld_savestart_dt
                           and v.cal_dt <
                               curval_res_capacity_gtres.eff_start_dt -- modified bb 1/10/2018 to accomadate halt_dt change
                        /* and v.cal_dt <= (CASE
                          WHEN (curval_res_capacity_gtres.HALT_FLG = 'Y' AND
                               curval_res_capacity_gtres.HALT_END_DT <
                               curval_res_capacity_gtres.eff_start_dt) THEN
                           curval_res_capacity_gtres.HALT_END_DT - 1
                          ELSE
                           curval_res_capacity_gtres.eff_start_dt
                        END)*/
                        ) rc,
                       eng_enterprise_ref_snp_vw eer --added by SA on 12/22
                 WHERE eer.enterprise = i_enterprise_nm
                   AND eer.ent_res_flg like '%:' || rc.RES_AREA || ':%';
            
              ld_savestart_dt := curval_res_capacity_gtres.eff_start_dt;
              ln_cap_qty      := curval_res_capacity_gtres.daily_cap_qty;
              -- dbms_output.put_line('ld_savestart_dt is ' || ld_savestart_dt);
              -- dbms_output.put_line('ld_end_dt is ' ||
              --                    curval_res_capacity_gtres.eff_start_dt);
            
              -- dbms_output.put_line('ln_cap_qty is ' || ln_cap_qty);
              /*IF curval_res_capacity_gtres.halt_end_dt <
                 curval_res_capacity_gtres.eff_start_dt THEN
                ln_halt_cond := 1;
                --dbms_output.put_line('ln_halt_cond_reached');
                exit;
              
              END IF;*/
            
            END IF;
          
            IF ln_counter = ln_cnt_rows THEN
              /*  dbms_output.put_line('ln_counter is  ' || ln_counter);
                dbms_output.put_line('ld_start_dt is ' || ld_savestart_dt);
                dbms_output.put_line('ld_end_dt is ' || 'end');
              */
              INSERT INTO res_cap_src
                (res_nm,
                 eng_res_nm,
                 site_num,
                 res_area,
                 res_type,
                 --res_cnt,
                 eff_res_cnt,
                 cap_uom,
                 bkt_uom,
                 day_of_wk,
                 wk_num,
                 qtr_num,
                 yr_num,
                 vnd_num,
                 enterprise,
                 source_dt,
                 row_source)
                SELECT rc.res_nm,
                       rc.eng_res_nm,
                       rc.site_num,
                       rc.RES_AREA,
                       rc.RES_TYPE,
                       rc.EFF_RES_CNT,
                       rc.CAP_UOM,
                       rc.Bkt_UOM,
                       rc.WORK_DAY,
                       rc.Work_WEEK,
                       rc.QUARTER,
                       rc.YEAR,
                       rc.VND_NUM,
                       eer.enterprise,
                       sysdate,
                       'RCS-10'
                  FROM (SELECT assmbly.res_nm AS RES_NM,
                               assmbly.res_nm || '_' || assmbly.eng_site_num AS ENG_RES_NM,
                               assmbly.eng_site_num AS SITE_NUM,
                               assmbly.res_cd AS RES_AREA,
                               assmbly.res_cd AS RES_TYPE,
                               assmbly.daily_cap_qty AS EFF_RES_CNT,
                               assmbly.cap_uom AS CAP_UOM,
                               'DAILY' as Bkt_UOM,
                               v.day_of_wk AS WORK_DAY,
                               v.wk_num AS Work_WEEK,
                               v.qtr_num AS QUARTER,
                               v.yr_num AS YEAR,
                               assmbly.vnd_num
                          FROM scmdata.DMCLS_SNP_VW v,
                               (select dtl.res_cd,
                                       dtl.res_nm,
                                       dtl.vnd_num,
                                       dtl.vnd_nm,
                                       dtl.eff_start_dt,
                                       ln_cap_qty daily_cap_qty,
                                       mstr.cap_uom AS CAP_UOM,
                                       smr.site_num,
                                       smr.eng_site_num,
                                       'DAILY' as Bkt_UOM
                                  from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                                       ASSY_DAILY_CAP_MASTER_SNP  mstr,
                                       ref_data.site_master_ref   smr
                                 where dtl.res_nm = mstr.res_nm
                                   and dtl.res_cd = mstr.res_cd
                                   and dtl.vnd_num = mstr.vnd_num
                                   and mstr.PLANNABLE_FLG = 'Y'
                                   and dtl.vnd_num = smr.
                                 site_num -- ltrim(smr.site_num, '0')
                                   and smr.plannable_flg = 'Y'
                                   and dtl.res_cd =
                                       curval_res_capacity_gtres.res_cd
                                   and dtl.res_nm =
                                       curval_res_capacity_gt.res_nm
                                   and dtl.vnd_num =
                                       curval_res_capacity_gtres.vnd_num
                                   and rownum < 2) assmbly
                         where v.cal_dt >= ld_savestart_dt
                           and v.cal_dt <= ld_planend_dt) rc, -- modified bb 1/10/2018 to accomadate halt_dt change
                       eng_enterprise_ref_snp_vw eer --added by SA on 12/22
                 WHERE eer.enterprise = i_enterprise_nm
                   AND eer.ent_res_flg like '%:' || rc.RES_AREA || ':%';
            
            END IF;
          
          END LOOP; -- end inner cursor
        
        END IF; -- halt_end_dt is further out planning can start
      
      ELSE
        -- halt_flg is N just continue normal processing
      
        FOR curval_res_capacity_gtres IN cur_res_capacity_gtres LOOP
          ln_counter := ln_counter + 1;
        
          IF cur_res_capacity_gtres%ROWCOUNT = 1 THEN
          
            select dtl.daily_cap_qty rptdtqty
              INTO ln_start_cap_qty
              from ASSY_DAILY_CAP_DETAILS_SNP dtl
             where dtl.res_cd = curval_res_capacity_gtres.res_cd
               and dtl.res_nm = curval_res_capacity_gt.res_nm
               and dtl.vnd_num = curval_res_capacity_gtres.vnd_num
               and dtl.eff_start_dt =
                   (select max(dtl.eff_start_dt)
                      from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                           ASSY_DAILY_CAP_MASTER_SNP  mstr,
                           ref_data.site_master_ref   smr
                     where dtl.res_nm = mstr.res_nm
                       and dtl.res_cd = mstr.res_cd
                       and dtl.vnd_num = mstr.vnd_num
                       and mstr.PLANNABLE_FLG = 'Y'
                       and dtl.vnd_num = smr.site_num -- ltrim(smr.site_num, '0')
                       and smr.plannable_flg = 'Y'
                       and dtl.res_cd = curval_res_capacity_gtres.res_cd
                       and dtl.res_nm = curval_res_capacity_gt.res_nm
                       and dtl.vnd_num = curval_res_capacity_gtres.vnd_num
                       and dtl.eff_start_dt < ld_planstart_dt);
          
            INSERT INTO res_cap_src
              (res_nm,
               eng_res_nm,
               site_num,
               res_area,
               res_type,
               --res_cnt,
               eff_res_cnt,
               cap_uom,
               bkt_uom,
               day_of_wk,
               wk_num,
               qtr_num,
               yr_num,
               vnd_num,
               enterprise,
               source_dt,
               row_source)
              SELECT rc.res_nm,
                     rc.eng_res_nm,
                     rc.site_num,
                     rc.RES_AREA,
                     rc.RES_TYPE,
                     rc.EFF_RES_CNT,
                     rc.CAP_UOM,
                     rc.Bkt_UOM,
                     rc.WORK_DAY,
                     rc.Work_WEEK,
                     rc.QUARTER,
                     rc.YEAR,
                     rc.VND_NUM,
                     eer.enterprise,
                     sysdate,
                     'RCS-11'
                FROM (SELECT assmbly.res_nm AS RES_NM,
                             assmbly.res_nm || '_' || assmbly.eng_site_num AS ENG_RES_NM,
                             assmbly.eng_site_num AS SITE_NUM,
                             assmbly.res_cd AS RES_AREA,
                             assmbly.res_cd AS RES_TYPE,
                             assmbly.daily_cap_qty AS EFF_RES_CNT,
                             assmbly.cap_uom AS CAP_UOM,
                             'DAILY' as Bkt_UOM,
                             v.day_of_wk AS WORK_DAY,
                             v.wk_num AS Work_WEEK,
                             v.qtr_num AS QUARTER,
                             v.yr_num AS YEAR,
                             assmbly.vnd_num
                        FROM scmdata.DMCLS_SNP_VW v,
                             (select dtl.res_cd,
                                     dtl.res_nm,
                                     dtl.vnd_num,
                                     dtl.vnd_nm,
                                     dtl.eff_start_dt,
                                     ln_start_cap_qty daily_cap_qty,
                                     mstr.cap_uom AS CAP_UOM,
                                     smr.site_num,
                                     smr.eng_site_num,
                                     'DAILY' as Bkt_UOM,
                                     mstr.halt_flg,
                                     mstr.halt_end_dt
                                from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                                     ASSY_DAILY_CAP_MASTER_SNP  mstr,
                                     ref_data.site_master_ref   smr
                               where dtl.res_nm = mstr.res_nm
                                 and dtl.res_cd = mstr.res_cd
                                 and dtl.vnd_num = mstr.vnd_num
                                 and mstr.PLANNABLE_FLG = 'Y'
                                 and dtl.vnd_num = smr.
                               site_num -- ltrim(smr.site_num, '0')
                                 and smr.plannable_flg = 'Y'
                                 and dtl.res_cd =
                                     curval_res_capacity_gtres.res_cd
                                 and dtl.res_nm =
                                     curval_res_capacity_gt.res_nm
                                 and dtl.vnd_num =
                                     curval_res_capacity_gtres.vnd_num
                                 and rownum < 2) assmbly
                       where v.cal_dt >= ld_planstart_dt
                         and v.cal_dt <
                             (curval_res_capacity_gtres.eff_start_dt)) rc,
                     eng_enterprise_ref_snp_vw eer --added by SA on 12/22
               WHERE eer.enterprise = i_enterprise_nm
                 AND eer.ent_res_flg like '%:' || rc.RES_AREA || ':%';
            --least((nvl(curval.halt_end_dt, to_date('9/9/9999', 'MM/DD/YYYY')), curval_res_capacity_gtres.eff_start_dt)); --to_date('10/20/2017', 'MM/DD/YYYY');
          
            ld_savestart_dt := curval_res_capacity_gtres.eff_start_dt;
            ln_cap_qty      := curval_res_capacity_gtres.daily_cap_qty;
          
          ELSE
            -- from row 2 onwards in curval_res_capacity_gtres
          
            INSERT INTO res_cap_src
              (res_nm,
               eng_res_nm,
               site_num,
               res_area,
               res_type,
               --res_cnt,
               eff_res_cnt,
               cap_uom,
               bkt_uom,
               day_of_wk,
               wk_num,
               qtr_num,
               yr_num,
               vnd_num,
               enterprise,
               source_dt,
               row_source)
              SELECT rc.res_nm,
                     rc.eng_res_nm,
                     rc.site_num,
                     rc.RES_AREA,
                     rc.RES_TYPE,
                     rc.EFF_RES_CNT,
                     rc.CAP_UOM,
                     rc.Bkt_UOM,
                     rc.WORK_DAY,
                     rc.Work_WEEK,
                     rc.QUARTER,
                     rc.YEAR,
                     rc.VND_NUM,
                     eer.enterprise,
                     sysdate,
                     'RCS-12'
                FROM (SELECT assmbly.res_nm AS RES_NM,
                             assmbly.res_nm || '_' || assmbly.eng_site_num AS ENG_RES_NM,
                             assmbly.eng_site_num AS SITE_NUM,
                             assmbly.res_cd AS RES_AREA,
                             assmbly.res_cd AS RES_TYPE,
                             assmbly.daily_cap_qty AS EFF_RES_CNT,
                             assmbly.cap_uom AS CAP_UOM,
                             'DAILY' as Bkt_UOM,
                             v.day_of_wk AS WORK_DAY,
                             v.wk_num AS Work_WEEK,
                             v.qtr_num AS QUARTER,
                             v.yr_num AS YEAR,
                             assmbly.vnd_num
                        FROM scmdata.DMCLS_SNP_VW v,
                             (select dtl.res_cd,
                                     dtl.res_nm,
                                     dtl.vnd_num,
                                     dtl.vnd_nm,
                                     dtl.eff_start_dt,
                                     ln_cap_qty daily_cap_qty,
                                     mstr.cap_uom AS CAP_UOM,
                                     smr.site_num,
                                     smr.eng_site_num,
                                     'DAILY' as Bkt_UOM
                                from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                                     ASSY_DAILY_CAP_MASTER_SNP  mstr,
                                     ref_data.site_master_ref   smr
                               where dtl.res_nm = mstr.res_nm
                                 and dtl.res_cd = mstr.res_cd
                                 and dtl.vnd_num = mstr.vnd_num
                                 and mstr.PLANNABLE_FLG = 'Y'
                                 and dtl.vnd_num = smr.
                               site_num -- ltrim(smr.site_num, '0')
                                 and smr.plannable_flg = 'Y'
                                 and dtl.res_cd =
                                     curval_res_capacity_gtres.res_cd
                                 and dtl.res_nm =
                                     curval_res_capacity_gt.res_nm
                                 and dtl.vnd_num =
                                     curval_res_capacity_gtres.vnd_num
                                    --and dtl.eff_start_dt = curval_res_capacity_gtres.eff_start_dt
                                 and rownum < 2) assmbly
                       where v.cal_dt >= ld_savestart_dt
                         and v.cal_dt <
                             curval_res_capacity_gtres.eff_start_dt) rc,
                     eng_enterprise_ref_snp_vw eer --added by SA on 12/22
               WHERE eer.enterprise = i_enterprise_nm --added by SA by 1/11
                 AND eer.ent_res_flg like '%:' || rc.RES_AREA || ':%';
          
            ld_savestart_dt := curval_res_capacity_gtres.eff_start_dt;
            ln_cap_qty      := curval_res_capacity_gtres.daily_cap_qty;
          
          END IF;
        
          IF ln_counter = ln_cnt_rows THEN
          
            INSERT INTO res_cap_src
              (res_nm,
               eng_res_nm,
               site_num,
               res_area,
               res_type,
               --res_cnt,
               eff_res_cnt,
               cap_uom,
               bkt_uom,
               day_of_wk,
               wk_num,
               qtr_num,
               yr_num,
               vnd_num,
               enterprise,
               source_dt,
               row_source)
              SELECT rc.res_nm,
                     rc.eng_res_nm,
                     rc.site_num,
                     rc.RES_AREA,
                     rc.RES_TYPE,
                     rc.EFF_RES_CNT,
                     rc.CAP_UOM,
                     rc.Bkt_UOM,
                     rc.WORK_DAY,
                     rc.Work_WEEK,
                     rc.QUARTER,
                     rc.YEAR,
                     rc.VND_NUM,
                     eer.enterprise,
                     sysdate,
                     'RCS-13'
                FROM (SELECT assmbly.res_nm AS RES_NM,
                             assmbly.res_nm || '_' || assmbly.eng_site_num AS ENG_RES_NM,
                             assmbly.eng_site_num AS SITE_NUM,
                             assmbly.res_cd AS RES_AREA,
                             assmbly.res_cd AS RES_TYPE,
                             assmbly.daily_cap_qty AS EFF_RES_CNT,
                             assmbly.cap_uom AS CAP_UOM,
                             'DAILY' as Bkt_UOM,
                             v.day_of_wk AS WORK_DAY,
                             v.wk_num AS Work_WEEK,
                             v.qtr_num AS QUARTER,
                             v.yr_num AS YEAR,
                             assmbly.vnd_num
                        FROM scmdata.DMCLS_SNP_VW v,
                             (select dtl.res_cd,
                                     dtl.res_nm,
                                     dtl.vnd_num,
                                     dtl.vnd_nm,
                                     dtl.eff_start_dt,
                                     ln_cap_qty daily_cap_qty,
                                     mstr.cap_uom AS CAP_UOM,
                                     smr.site_num,
                                     smr.eng_site_num,
                                     'DAILY' as Bkt_UOM
                              -- select resource_nm,vendor_id,max(eff_start_dt) -- 1028 rows
                                from ASSY_DAILY_CAP_DETAILS_SNP dtl,
                                     ASSY_DAILY_CAP_MASTER_SNP  mstr,
                                     ref_data.site_master_ref   smr
                               where dtl.res_nm = mstr.res_nm
                                 and dtl.res_cd = mstr.res_cd
                                 and dtl.vnd_num = mstr.vnd_num
                                 and mstr.PLANNABLE_FLG = 'Y'
                                 and dtl.vnd_num = smr.
                               site_num -- ltrim(smr.site_num, '0')
                                 and smr.plannable_flg = 'Y'
                                 and dtl.res_cd =
                                     curval_res_capacity_gtres.res_cd
                                 and dtl.res_nm =
                                     curval_res_capacity_gt.res_nm
                                 and dtl.vnd_num =
                                     curval_res_capacity_gtres.vnd_num
                                    --and dtl.eff_start_dt = curval_res_capacity_gtres.eff_start_dt
                                 and rownum < 2) assmbly
                       where v.cal_dt >= ld_savestart_dt
                         and v.cal_dt <= ld_planend_dt) rc,
                     eng_enterprise_ref_snp_vw eer --added by SA on 12/22
               WHERE eer.enterprise = i_enterprise_nm
                 AND eer.ent_res_flg like '%:' || rc.RES_AREA || ':%';
          
          END IF;
        
        END LOOP; -- end inner cursor
      
      END IF; -- halt_flg is N
    
    END LOOP; -- end outer cursor
  
    commit;
  
    ----Below logic handles for res_grp_cd for assembly
    ---Added by VS 12/19/2017
    INSERT INTO RES_CAP_SRC
      (res_nm,
       eng_res_nm,
       site_num,
       res_area,
       res_type,
       eff_res_cnt,
       cap_uom,
       bkt_uom,
       day_of_wk,
       wk_num,
       qtr_num,
       yr_num,
       vnd_num,
       enterprise,
       source_dt,
       etl_dt,
       row_source)
      select distinct asy.res_grp_cd res_nm,
                      asy.res_grp_cd || '_' || smr.eng_site_num eng_res_nm,
                      rcs.site_num,
                      rcs.res_area,
                      rcs.res_type,
                      sum(rcs.eff_res_cnt) over(partition by rcs.site_num, rcs.yr_num, rcs.qtr_num, rcs.wk_num, rcs.day_of_wk) eff_res_cnt,
                      rcs.cap_uom,
                      rcs.bkt_uom,
                      rcs.day_of_wk,
                      rcs.wk_num,
                      rcs.qtr_num,
                      rcs.yr_num,
                      rcs.vnd_num,
                      rcs.enterprise,
                      sysdate,
                      rcs.etl_dt,
                      'RCS-14'
        from res_cap_src                 rcs,
             assy_daily_cap_master_snp   asy,
             ref_data.site_master_ref    smr,
             ref_data.eng_enterprise_ref eer --added by SA on 12/22
       where asy.vnd_num = smr.site_num
         and asy.res_grp_cd is not null
         and eer.enterprise = rcs.enterprise --added by SA on 12/22
         and eer.enterprise = i_enterprise_nm --added by SA on 12/22
         and eer.ent_res_flg like '%:' || rcs.RES_AREA || ':%' --added by SA on 12/22
         and rcs.site_num = smr.eng_site_num
         and rcs.res_nm = asy.res_nm
       order by asy.res_grp_cd,
                rcs.site_num,
                rcs.yr_num,
                rcs.qtr_num,
                rcs.wk_num;
  
    commit;
  
  END;

  --************************************************************
  --Procedure to load data into RES_HW_ALT_SRC table
  -- This procedure is used to load data for Sort and Test resources
  --************************************************************

  Procedure load_RES_HW_ALT_SRC(i_enterprise_nm IN VARCHAR2) IS
  
  BEGIN
  
    frmdata.logs.begin_log('Start RES_HW_ALT_SRC load from SNP to SRC');
    frmdata.delete_data('RES_HW_ALT_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    lv_row_source := 'Z001';
  
    -- Loading data for Sort and test resources
  
    INSERT INTO RES_HW_ALT_SRC
      (ENTERPRISE,
       PROD_RTE_SEQ_ID,
       ENG_RTE_ID,
       ENG_OPER_SEQ,
       HW_TYPE,
       PRIM_HW_NM,
       PRIM_HW_REQUIRED_QTY,
       ALT_HW_NM,
       ALT_HW_REQUIRED_QTY,
       HW_SET_ID,
       ETL_DT,
       SOURCE_DT,
       ROW_SOURCE)
      select distinct a.enterprise,
                      a.prod_rte_seq_id,
                      a.eng_rte_id,
                      a.eng_oper_seq,
                      c.hw_type,
                      a.alt_res_prim_res_nm prim_hw_nm,
                      b.required_qty        prim_hw_qty,
                      c.hw_nm               alt_hw_nm,
                      c.required_qty        alt_hw_qty,
                      c.hw_set_id,
                      b.etl_dt,
                      sysdate,
                      'HWA-01'
        from scmdata.RES_RTE_ASN_DTL_SRC a,
             RES_ATTR_SRC                b,
             Hardware_Alternates_Snp_Vw  c
       where b.res_type = 'HARDWARE_SET' ------------ADDED BY VS 01/11/18
         and a.alt_res_prim_res_nm != c.hw_nm
         and b.res_nm = c.hw_set_id
         and b.hw_type = c.hw_type
         and a.alt_res_type like '%' || b.hw_type
         and a.prod_rte_seq_id = b.prod_rte_seq_id;
  
    Commit; /* remove later  */
    frmdata.logs.info('RES_HW_ALT_SRC load from SNP to SRC is completed');
  
  END load_RES_HW_ALT_SRC;

  ------FAB-Resources Logic----Added by VS on 10/17
  Procedure load_RES_FAB_ASN_SRC(i_enterprise_nm IN VARCHAR2) is
    lv_row_source VARCHAR2(100) := NULL;
  BEGIN
    frmdata.logs.begin_log('Start RES_FAB_ASN_SRC load from SNP to SRC');
    frmdata.delete_data('RES_FAB_ASN_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
    IF i_enterprise_nm = 'SCP_STARTS' THEN
      lv_row_source := 'FA001';
    
      INSERT INTO RES_FAB_ASN_SRC
        SELECT DISTINCT eer.enterprise,
                        fpr.mfg_part_num,
                        pms.matl_type_cd,
                        fpr.fab_num,
                        smr.eng_site_num,
                        rs.eng_rte_id,
                        rs.eng_oper_seq,
                        fpr.fab_proc_grp_nm,
                        fpr.fab_proc_grp_nm,
                        NULL alt_type,
                        '1' WIP_UPH,
                        'FAB' RES_TYPE,
                        fpr.updt_dt,
                        SYSDATE,
                        'RFA-01'
          FROM ref_data.eng_enterprise_ref eer,
               ref_data.site_master_ref    smr,
               fab_part_res_asn_snp_vw     fpr,
               part_master_src             pms,
               rte_src                     rs
         WHERE eer.enterprise = i_enterprise_nm
           AND pms.mfg_part_num = fpr.mfg_part_num
           AND pms.fab_proc_grp_nm = fpr.fab_proc_grp_nm
           AND rs.mfg_part_num = fpr.mfg_part_num
           AND rs.eng_site_num = fpr.fab_num
           AND rs.step_nm like '%FAB%'
           AND (pms.dmd_flg = 'Y' or wip_flg = 'Y')
           AND pms.part_excp_flg = 'N'
           AND smr.site_num = fpr.fab_num
           AND smr.plannable_flg = 'Y'
        UNION
        SELECT DISTINCT eer.enterprise,
                        fpr.mfg_part_num,
                        pms.matl_type_cd,
                        fpr.fab_num,
                        smr.eng_site_num,
                        rs.eng_rte_id,
                        rs.eng_oper_seq,
                        fpr.fab_proc_comb_grp_nm,
                        fpr.fab_proc_grp_nm,
                        NULL alt_type,
                        '1' WIP_UPH,
                        'FAB' RES_TYPE,
                        fpr.updt_dt,
                        SYSDATE,
                        'RFA-01-1'
          FROM ref_data.eng_enterprise_ref eer,
               ref_data.site_master_ref    smr,
               fab_part_res_asn_snp_vw     fpr,
               part_master_src             pms,
               rte_src                     rs
         WHERE eer.enterprise = i_enterprise_nm
           AND pms.mfg_part_num = fpr.mfg_part_num
           AND pms.fab_proc_grp_nm = fpr.fab_proc_grp_nm
           AND rs.mfg_part_num = fpr.mfg_part_num
           AND rs.eng_site_num = fpr.fab_num
           AND rs.step_nm LIKE '%FAB%'
           AND (pms.dmd_flg = 'Y' OR wip_flg = 'Y')
           AND pms.part_excp_flg = 'N'
           AND smr.site_num = fpr.fab_num
           AND smr.plannable_flg = 'Y'
        UNION
        SELECT DISTINCT eer.enterprise,
                        fpr.mfg_part_num,
                        pms.matl_type_cd,
                        fpr.fab_num,
                        smr.eng_site_num,
                        rs.eng_rte_id,
                        rs.eng_oper_seq,
                        fpr.fab_grp_nm,
                        fpr.fab_proc_grp_nm,
                        NULL alt_type,
                        '1' WIP_UPH,
                        'FAB' RES_TYPE,
                        fpr.updt_dt,
                        SYSDATE,
                        'RFA-01-2'
          FROM ref_data.eng_enterprise_ref eer,
               ref_data.site_master_ref    smr,
               fab_part_res_asn_snp_vw     fpr,
               part_master_src             pms,
               rte_src                     rs
         WHERE eer.enterprise = i_enterprise_nm
           AND pms.mfg_part_num = fpr.mfg_part_num
           AND pms.fab_proc_grp_nm = fpr.fab_proc_grp_nm
           AND rs.mfg_part_num = fpr.mfg_part_num
           AND rs.eng_site_num = fpr.fab_num
           AND rs.step_nm LIKE '%FAB%'
           AND (pms.dmd_flg = 'Y' OR wip_flg = 'Y')
           AND pms.part_excp_flg = 'N'
           AND smr.site_num = fpr.fab_num
           AND smr.plannable_flg = 'Y';
    
      --Updating the ALT_TYPE column in RES_FAB_ASN_SRC if PRIM or SIMUL
    
      UPDATE RES_FAB_ASN_SRC
         SET alt_type = 'PRIM'
       WHERE res_nm = prim_res_nm;
    
      UPDATE RES_FAB_ASN_SRC
         SET alt_type = 'SIMUL'
       WHERE res_nm <> prim_res_nm;
    
      COMMIT;
    END IF;
  
  END load_RES_FAB_ASN_SRC;

  ----------Populate NON ATE resources--------

  Procedure load_RES_NONATE_ATTR_SRC(i_enterprise_nm IN VARCHAR2) is
    CURSOR cur_attr IS
      SELECT DISTINCT eer.enterprise,
                      ralt.prod_rte_seq_id,
                      ralt.prim_res_set_id,
                      ralt.res_set_id,
                      ralt.prio_cd,
                      hss.hw_type res_type, ---added res_type, res_nm to find the alt_res, alt_rte_flg 11/09/17 by VS
                      hss.hw_nm res_nm,
                      (SELECT attr_val_num
                         FROM resource_attr_snp_vw
                        WHERE attr_nm = 'TURN_TIME'
                          AND ralt.attr_set_id = attr_set_id) attr_numeric_value_tt,
                      (SELECT attr_val_num
                         FROM resource_attr_snp_vw
                        WHERE attr_nm = 'PARTS_PER_BOARD'
                          AND ralt.attr_set_id = attr_set_id) attr_numeric_value_ppb,
                      (SELECT attr_val_num
                         FROM resource_attr_snp_vw
                        WHERE attr_nm = 'BOARDS_PER_OVEN'
                          AND ralt.attr_set_id = attr_set_id) attr_numeric_value_bpo,
                      ralt.etl_dt,
                      SYSDATE source_dt,
                      'RNA-01' row_source
        FROM ref_data.eng_enterprise_ref eer,
             Res_Alternates_Snp_vw       ralt,
             resource_attr_snp_vw        ratt,
             bill_of_resources_snp_vw    bor,
             prod_res_relation_snp_vw    prr,
             hardware_set_snp_vw         hss
       WHERE eer.enterprise = i_enterprise_nm
         AND ralt.res_set_id = bor.res_set_id
         AND ralt.attr_set_id = ratt.attr_set_id
         AND ralt.prod_rte_seq_id = prr.prod_rte_seq_id
         AND ralt.prim_res_set_id = prr.prim_res_set_id
         AND bor.res_nm = hss.hw_set_id
         AND prr.res_area like 'NON ATE FT'
         AND eer.ent_res_flg like '%:' || prr.RES_AREA || ':%' --added by SA on 12/22
      union
      SELECT DISTINCT eer.enterprise,
                      ralt.prod_rte_seq_id,
                      ralt.prim_res_set_id,
                      ralt.res_set_id,
                      ralt.prio_cd,
                      bor.res_type, ---added res_type, res_nm to find the alt_res, alt_rte_flg 11/09/17 by VS
                      bor.res_nm,
                      (SELECT attr_val_num
                         FROM resource_attr_snp_vw
                        WHERE attr_nm = 'TURN_TIME'
                          AND ralt.attr_set_id = attr_set_id) attr_numeric_value_tt,
                      (SELECT attr_val_num
                         FROM resource_attr_snp_vw
                        WHERE attr_nm = 'PARTS_PER_BOARD'
                          AND ralt.attr_set_id = attr_set_id) attr_numeric_value_ppb,
                      (SELECT attr_val_num
                         FROM resource_attr_snp_vw
                        WHERE attr_nm = 'BOARDS_PER_OVEN'
                          AND ralt.attr_set_id = attr_set_id) attr_numeric_value_bpo,
                      ralt.etl_dt,
                      SYSDATE source_dt,
                      'RNA-02' row_source
        FROM ref_data.eng_enterprise_ref eer,
             Res_Alternates_Snp_vw       ralt,
             resource_attr_snp_vw        ratt,
             bill_of_resources_snp_vw    bor,
             prod_res_relation_snp_vw    prr
       WHERE eer.enterprise = i_enterprise_nm
         AND ralt.res_set_id = bor.res_set_id
         AND ralt.attr_set_id = ratt.attr_set_id
         AND ralt.prod_rte_seq_id = prr.prod_rte_seq_id
         AND ralt.prim_res_set_id = prr.prim_res_set_id
         AND bor.res_type like 'BIOV'
         AND prr.res_area like 'NON ATE FT'
         AND eer.ent_res_flg like '%:' || prr.RES_AREA || ':%' --added by SA on 12/22
       ORDER BY prod_rte_seq_id, prim_res_set_id;
  
    TYPE tab_cur_attr IS TABLE OF cur_attr%ROWTYPE;
    curval_attr tab_cur_attr := tab_cur_attr();
  BEGIN
    frmdata.logs.begin_log('Start RES_NONATE_ATTR_SRC load from SNP to SRC');
    frmdata.delete_data('RES_NONATE_ATTR_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
    lv_row_source := 'N001';
    OPEN cur_attr;
    LOOP
      FETCH cur_attr BULK COLLECT
        INTO curval_attr LIMIT 1000;
    
      FORALL i IN 1 .. curval_attr.count
        INSERT INTO res_nonate_attr_src
        VALUES
          (curval_attr(i).enterprise,
           curval_attr(i).prod_rte_seq_id,
           curval_attr(i).prim_res_set_id,
           curval_attr(i).res_set_id,
           curval_attr(i).prio_cd,
           curval_attr(i).res_type,
           curval_attr(i).res_nm,
           curval_attr(i).attr_numeric_value_tt,
           curval_attr(i).attr_numeric_value_ppb,
           curval_attr(i).attr_numeric_value_bpo,
           curval_attr(i).etl_dt,
           curval_attr(i).source_dt,
           curval_attr(i).row_source);
      EXIT WHEN cur_attr%NOTFOUND;
    END LOOP;
  
    COMMIT;
  
  END load_RES_NONATE_ATTR_SRC;

  --********************************************************************
  -- TO load data into  Assy_Part_Vnd_Src table from SNP source
  -- This procedure is used to load data for Assembly/Bump/Diecoat resources
  --*********************************************************************

  PROCEDURE load_Assy_Part_Vnd_Src(i_enterprise_nm IN VARCHAR2) IS
  BEGIN
  
    frmdata.logs.begin_log('Start ASSY_PART_VND_SRC load from SNP to SRC');
    frmdata.delete_data('ASSY_PART_VND_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    lv_row_source := 'APV001';
  
    -- Remove outerjoins below once good data is available below check with Sarveswar if this statement is true
    --need change here
    -- Mapping For assembly resources
    Insert into Assy_Part_Vnd_Src
      (Matl_type_cd,
       Mfg_part_num,
       Vnd_num,
       Eng_Site_Num,
       Eng_Rte_id,
       Eng_oper_seq,
       Run_time_per,
       Res_cd,
       Res_Nm,
       Res_Grp_cd,
       Cap_Uom,
       Plannable_Flg,
       Halt_flg,
       Halt_end_dt,
       Vnd_Stat,
       Enterprise,
       Etl_Dt,
       Source_Dt,
       Row_Source)
      SELECT distinct assyrte.matl_type_cd,
                      assyrte.Mfg_part_num,
                      assyrte.Vnd_num,
                      --Vendor_Status,
                      assyrte.sitenum,
                      assyrte.Eng_Rte_id,
                      assyrte.Eng_oper_seq,
                      1                     as Run_time_per,
                      assyrte.Res_cd,
                      assyrte.Res_Nm,
                      assyrte.Res_Grp_cd,
                      assyrte.Cap_Uom,
                      assyrte.plannable_flg,
                      assyrte.Halt_flg,
                      assyrte.Halt_end_dt,
                      b.bom_status_cd       vendor_status,
                      assyrte.enterprise,
                      assyrte.etl_dt        source_dt,
                      sysdate,
                      'APV-01'
        FROM (SELECT assy.*, r.eng_rte_id, r.eng_oper_seq, r.eng_site_num
                from (select distinct dtl.res_cd,
                                      dtl.res_nm,
                                      dtl.vnd_num,
                                      mstr.res_grp_cd,
                                      mstr.cap_uom,
                                      mstr.plannable_flg,
                                      mstr.halt_flg,
                                      mstr.halt_end_dt,
                                      smr.eng_site_num   sitenum,
                                      eer.enterprise,
                                      mstr.updt_dt       etl_dt,
                                      pas.mfg_part_num,
                                      pas.matl_type_cd,
                                      pas.pkg_type,
                                      pas.pkg_cd
                      --, bom.*
                        from part_attr_src pas,
                             (select distinct res_cd, res_nm, vnd_num
                                from assy_daily_cap_details_snp
                               where res_cd = 'PKG') dtl,
                             assy_daily_cap_master_snp mstr,
                             ref_data.site_master_ref smr,
                             ref_data.eng_enterprise_ref eer
                       where eer.enterprise = i_enterprise_nm
                         AND eer.ent_res_flg like '%:' || dtl.res_cd || ':%' --addedd by SA on 12/22
                         and pas.pkg_cd = dtl.res_nm
                         and pas.matl_type_cd = 'ZASY'
                         and dtl.vnd_num = smr.site_num
                         and smr.plannable_flg = 'Y'
                         and dtl.res_nm = mstr.res_nm
                         and dtl.res_cd = mstr.res_cd
                         and dtl.vnd_num = mstr.vnd_num) assy,
                     rte_src r
               where assy. mfg_part_num = r.mfg_part_num
                 and assy.matl_type_cd = r.matl_type_cd
                    --and    bb.vendor_id       = r.eng_site_num
                 and assy.sitenum = r.eng_site_num) assyrte, -- 35,300 rows
             bom_master_src b
       where b.pdcd_matl_type_cd = assyrte.matl_type_cd
         and b.pdcd_mfg_part_num = assyrte.mfg_part_num
         and b.pdcd_site_num = assyrte.vnd_num;
  
    /* This one is for Bump */
    Insert into Assy_Part_Vnd_Src
      (Matl_type_cd,
       Mfg_part_num,
       Vnd_num,
       Eng_Site_Num,
       Eng_Rte_id,
       Eng_oper_seq,
       Run_time_per,
       Res_cd,
       Res_Nm,
       Res_Grp_cd,
       Cap_Uom,
       Plannable_Flg,
       Halt_flg,
       Halt_end_dt,
       Vnd_Stat,
       Enterprise,
       Etl_Dt,
       Source_Dt,
       Row_Source)
      select bmprtevend.matl_type_cd,
             bmprtevend.Mfg_part_num,
             bmprtevend.Vnd_num,
             --Vendor_Status,
             bmprtevend.sitenum,
             bmprtevend.Eng_Rte_id,
             bmprtevend.Eng_oper_seq,
             1 / (bomruntime.produced_qty) as Run_time_per,
             bmprtevend.Res_cd,
             bmprtevend.Res_Nm,
             bmprtevend.Res_Grp_cd,
             bmprtevend.Cap_Uom,
             bmprtevend.plannable_flg,
             bmprtevend.Halt_flg,
             bmprtevend.Halt_end_dt,
             bmprtevend.vendor_status,
             bmprtevend.enterprise,
             bmprtevend.etl_dt,
             sysdate,
             'APV-02'
        from (select distinct bmprte.matl_type_cd,
                              bmprte.Mfg_part_num,
                              bmprte.Vnd_num,
                              --Vendor_Status,
                              bmprte.sitenum,
                              bmprte.Eng_Rte_id,
                              bmprte.Eng_oper_seq,
                              bmprte.Res_cd,
                              bmprte.Res_Nm,
                              bmprte.Res_Grp_cd,
                              bmprte.Cap_Uom,
                              bmprte.plannable_flg,
                              bmprte.Halt_flg,
                              bmprte.Halt_end_dt,
                              bmprte.etl_dt,
                              bmprte.enterprise,
                              bom_stat.bom_status_cd vendor_status
                from (SELECT bmp.*,
                             r.eng_rte_id,
                             r.eng_oper_seq,
                             r.eng_site_num
                        from (select distinct dtl.res_cd,
                                              dtl.res_nm,
                                              dtl.vnd_num,
                                              mstr.res_grp_cd,
                                              mstr.cap_uom,
                                              mstr.plannable_flg,
                                              mstr.halt_flg,
                                              mstr.halt_end_dt,
                                              mstr.updt_dt       etl_dt,
                                              smr.eng_site_num   sitenum,
                                              pas.mfg_part_num,
                                              pas.matl_type_cd,
                                              pas.wfr_size,
                                              eer.enterprise
                              --, bom.*
                                from part_master_src pas,
                                     (select distinct res_cd, res_nm, vnd_num
                                        from assy_daily_cap_details_snp
                                       where res_cd = 'BUMP') dtl,
                                     assy_daily_cap_master_snp mstr,
                                     ref_data.site_master_ref smr,
                                     ref_data.eng_enterprise_ref eer
                               where eer.enterprise = i_enterprise_nm
                                 AND eer.ent_res_flg like
                                     '%:' || dtl.res_cd || ':%' --Added by SA on 1/3/2018
                                 and pas.wfr_size = dtl.res_nm
                                 and pas.matl_type_cd = 'ZBMP'
                                 and dtl.vnd_num = smr.site_num
                                 AND smr.plannable_flg = 'Y'
                                 AND dtl.res_nm = mstr.res_nm
                                 and dtl.res_cd = mstr.res_cd
                                 and dtl.vnd_num = mstr.vnd_num) bmp,
                             rte_src r
                       where bmp.mfg_part_num = r.mfg_part_num
                         and bmp.matl_type_cd = r.matl_type_cd
                            --and     bmp.vendor_id       = r.eng_site_num
                         and bmp.sitenum = r.eng_site_num) bmprte,
                     bom_master_src bom_stat
               where bom_stat.pdcd_matl_type_cd = bmprte.matl_type_cd
                 and bom_stat.pdcd_mfg_part_num = bmprte.mfg_part_num
                 and bom_stat.pdcd_site_num = bmprte.vnd_num) bmprtevend,
             (select pdcd_matl_type_cd,
                     pdcd_mfg_part_num,
                     pdcd_site_num,
                     max(pdcd_qty) produced_qty
                from bom_master_src
               where -- b.pdcd_matl_type_cd = bmp. matl_type_cd and b.pdcd_mfg_part_num = bmp.mfg_part_num
              -- and b.pdcd_site_num = bmp.vendor_id
               cmpnt_matl_type_cd = 'ZSRT'
               group by pdcd_matl_type_cd, pdcd_mfg_part_num, pdcd_site_num) bomruntime
       where bomruntime.pdcd_matl_type_cd = bmprtevend.matl_type_cd
         and bomruntime.pdcd_mfg_part_num = bmprtevend.mfg_part_num
         and bomruntime.pdcd_site_num = bmprtevend.vnd_num;
  
    -- For DIECOAT case 1 post DB diecoat
    -- For Diecoat step in Assembly Route:
  
    Insert into Assy_Part_Vnd_Src
      (Matl_type_cd,
       Mfg_part_num,
       Vnd_num,
       Eng_Site_Num,
       Eng_Rte_id,
       Eng_oper_seq,
       Run_time_per,
       Res_cd,
       Res_Nm,
       Res_Grp_cd,
       Cap_Uom,
       Plannable_Flg,
       Halt_flg,
       Halt_end_dt,
       Vnd_Stat,
       Enterprise,
       Etl_Dt,
       Source_Dt,
       ROW_SOURCE)
      select distinct postdc.pdcd_matl_type_cd,
                      postdc.pdcd_Mfg_part_num,
                      postdc.Vnd_num,
                      --Vendor_Status,
                      postdc.sitenum,
                      r.Eng_Rte_id,
                      r.Eng_oper_seq,
                      1                    as Run_time_per,
                      postdc.Res_cd,
                      postdc.Res_Nm,
                      postdc.Res_Grp_cd,
                      postdc.Cap_Uom,
                      postdc.plannable_flg,
                      postdc.Halt_flg,
                      postdc.Halt_end_dt,
                      postdc.bom_status_cd vendor_status,
                      postdc.enterprise,
                      postdc.etl_dt,
                      sysdate              source_dt,
                      'APV-03'
      /*select r.mfg_part_num,r.matl_type_cd,r.eng_site_num,r.eng_rte_id,r.eng_oper_seq,postdc.* */
        from rte_src r,
             (select distinct pdcd_mfg_part_num,
                              pdcd_matl_type_cd,
                              pdcd_site_num,
                              bom_status_cd,
                              b.res_cd,
                              b.res_nm,
                              b.vnd_num,
                              b.res_grp_cd,
                              b.cap_uom,
                              b.plannable_flg,
                              b.halt_flg,
                              b.halt_end_dt,
                              b.enterprise,
                              b.etl_dt,
                              b.sitenum
                from bom_master_src a,
                     REF_DATA.MATL_TYPE_cd_ref mtc,
                     (select distinct dtl.res_cd,
                                      dtl.res_nm,
                                      dtl.vnd_num,
                                      mstr.res_grp_cd,
                                      mstr.cap_uom,
                                      mstr.plannable_flg,
                                      mstr.halt_flg,
                                      mstr.halt_end_dt,
                                      smr.eng_site_num   sitenum,
                                      eer.enterprise,
                                      mstr.updt_dt       etl_dt
                        from (select distinct res_cd, res_nm, vnd_num, updt_dt
                                from assy_daily_cap_details_snp -- 1022 rows
                               where res_cd = 'DIECOAT') dtl,
                             assy_daily_cap_master_snp mstr,
                             ref_data.site_master_ref smr,
                             ref_data.eng_enterprise_ref eer
                       where dtl.vnd_num = smr.site_num
                         AND smr.plannable_flg = 'Y'
                         AND dtl.res_nm = mstr.res_nm
                         and dtl.res_cd = mstr.res_cd
                         and dtl.vnd_num = mstr.vnd_num
                         and eer.enterprise = i_enterprise_nm
                         AND eer.ent_res_flg like '%:' || dtl.res_cd || ':%' --Added by SA on 1/3/2018
                      ) b
               where cmpnt_matl_type_cd = 'ZUNB' --913 distinct pdcd_mfg_part_nums out of -- 5633 rows cmpnt_mfg_part_nums
                 and pdcd_matl_type_cd = 'ZASY'
                 and a.cmpnt_mfg_part_num = b.res_nm
                 AND mtc.matl_type_cd = A.pdcd_matl_type_cd --Added by SA on 1/11/2018
                 AND CASE
                       WHEN enterprise LIKE '%FP%' THEN
                        DECODE(enterprise,
                               'FP_EOL',
                               mtc.fp_eol_incl_flg,
                               'FP_SORT',
                               mtc.fp_sort_incl_flg,
                               'FP_MRP',
                               'Y')
                       ELSE
                        '1'
                     END = CASE
                       WHEN enterprise LIKE '%FP%' THEN
                        'Y'
                       ELSE
                        '1'
                     END
                 AND CASE
                       WHEN enterprise = 'FP_EOL' THEN
                        mtc.fp_eol_cmpnt_cd
                       WHEN enterprise = 'FP_SORT' THEN
                        mtc.fp_sort_cmpnt_cd
                       ELSE
                        '%:' || A.cmpnt_matl_type_cd || ':%'
                     END LIKE '%:' || A.cmpnt_matl_type_cd || ':%') postdc
       where r.area_cd in ('ASSY', 'DIECOAT')
         and postdc.pdcd_mfg_part_num = r.mfg_part_num
         and postdc.pdcd_matl_type_cd = r.matl_type_cd
         and postdc.pdcd_site_num = r.eng_site_num
      
      ;
  
    --Diecoat case 2 pre DB diecoat
    --For Diecoat step in Sort Route:
  
    Insert into Assy_Part_Vnd_Src
      (Matl_type_cd,
       Mfg_part_num,
       Vnd_num,
       Eng_Site_Num,
       Eng_Rte_Id,
       Eng_oper_seq,
       Run_time_per,
       Res_cd,
       Res_Nm,
       Res_Grp_cd,
       Cap_Uom,
       Plannable_Flg,
       Halt_flg,
       Halt_end_dt,
       Vnd_Stat,
       Enterprise,
       Etl_Dt,
       Source_Dt,
       Row_Source)
      select distinct predcrte.matl_type_cd,
                      predcrte.Mfg_part_num,
                      predcrte.Vnd_num,
                      --Vendor_Status,
                      predcrte.sitenum,
                      predcrte.Eng_Rte_id,
                      predcrte.Eng_oper_seq,
                      1                      as Run_time_per,
                      predcrte.Res_cd,
                      predcrte.Res_Nm,
                      predcrte.Res_Grp_cd,
                      predcrte.Cap_Uom,
                      predcrte.plannable_flg,
                      predcrte.Halt_flg,
                      predcrte.Halt_end_dt,
                      bom_stat.bom_status_cd vendor_status,
                      predcrte.enterprise,
                      predcrte.etl_dt,
                      sysdate,
                      'APV-04'
      --select *
        from (SELECT predc.*, r.eng_rte_id, r.eng_oper_seq, r.eng_site_num
                from (select distinct dtl.res_cd,
                                      dtl.res_nm,
                                      dtl.vnd_num,
                                      mstr.res_grp_cd,
                                      mstr.cap_uom,
                                      mstr.plannable_flg,
                                      mstr.halt_flg,
                                      mstr.halt_end_dt,
                                      mstr.updt_dt       etl_dt,
                                      smr.eng_site_num   sitenum,
                                      eer.enterprise,
                                      pas.mfg_part_num,
                                      pas.matl_type_cd,
                                      pas.die_coat
                        from part_attr_src pas,
                             (select distinct res_cd, res_nm, vnd_num
                                from assy_daily_cap_details_snp
                               where res_cd = 'DIECOAT') dtl,
                             assy_daily_cap_master_snp mstr,
                             ref_data.site_master_ref smr,
                             ref_data.eng_enterprise_ref eer
                       where eer.enterprise = i_enterprise_nm
                         and pas.die_coat = dtl.res_nm
                         and dtl.vnd_num = smr.site_num
                         AND smr.plannable_flg = 'Y'
                         AND dtl.res_nm = mstr.res_nm
                         and dtl.res_cd = mstr.res_cd
                         and dtl.vnd_num = mstr.vnd_num) predc,
                     
                     (select *
                        from rte_src
                       where step_nm in ('SORT', 'DIECOAT')) r
               where predc.mfg_part_num = r.mfg_part_num
                 and predc.matl_type_cd = r.matl_type_cd
              --and     predc.sitenum          = r.eng_site_num(+)
              -- Not inner joining sitenum as it is a valid use case that mfg_part_num and matl_type_cd match but the diecoat site is different from the route site
              ) predcrte,
             (select distinct pdcd_matl_type_cd,
                              pdcd_mfg_part_num,
                              pdcd_site_num,
                              bom_status_cd,
                              smr.eng_site_num,
                              cmpnt_matl_type_Cd ----added by SA 01/11/18
                from bom_master_src, ref_data.site_master_ref smr
               where bom_master_src.pdcd_site_num = smr.site_num
                 AND smr.plannable_flg = 'Y') bom_stat,
             ref_data.matl_type_cd_ref mtc
       where bom_stat.pdcd_matl_type_cd = predcrte.matl_type_cd
         and bom_stat.pdcd_mfg_part_num = predcrte.mfg_part_num
         and bom_stat.eng_site_num = predcrte.eng_site_num
         AND mtc.matl_type_cd = bom_stat.pdcd_matl_type_cd
         AND CASE
               WHEN enterprise LIKE '%FP%' THEN
                DECODE(enterprise,
                       'FP_EOL',
                       mtc.fp_eol_incl_flg,
                       'FP_SORT',
                       mtc.fp_sort_incl_flg,
                       'FP_MRP',
                       'Y')
               ELSE
                '1'
             END = CASE
               WHEN enterprise LIKE '%FP%' THEN
                'Y'
               ELSE
                '1'
             END
         AND CASE
               WHEN enterprise = 'FP_EOL' THEN
                mtc.fp_eol_cmpnt_cd
               WHEN enterprise = 'FP_SORT' THEN
                mtc.fp_sort_cmpnt_cd
               ELSE
                '%:' || bom_stat.cmpnt_matl_type_cd || ':%'
             END LIKE '%:' || bom_stat.cmpnt_matl_type_cd || ':%'; --predcrte.vnd_num
  
    commit;
  
  END;

  --********************************************************************
  -- TO load data into  Assy_Vnd_Alloc_Src table from SNP source
  -- This procedure is used to load data for Assembly/Bump/Diecoat resources
  --*********************************************************************

  PROCEDURE load_Assy_Vnd_Alloc_Src(i_enterprise_nm IN VARCHAR2) IS
  BEGIN
  
    frmdata.logs.begin_log('Start ASSY_VND_ALLOC_SRC load from SNP to SRC');
    frmdata.delete_data('ASSY_VND_ALLOC_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    lv_row_source := 'AV001';
    --need change here
    INSERT INTO Assy_Vnd_Alloc_Src
      (res_cd,
       res_nm,
       Cmpnt_Matl_type_cd,
       Cmpnt_Mfg_part_num,
       Pdcd_Matl_type_cd,
       Pdcd_Mfg_part_num,
       Vnd_num,
       Vnd_qual_stat,
       frm_site_num,
       cmpnt_site_num, --frm_eng_site_num,
       pdcd_site_num, --to_Eng_site_num,
       Orig_Alloc_pct,
       Eng_Alloc_pct,
       Alloc_start_dt,
       Alloc_end_dt,
       Active_flg,
       Enterprise,
       Etl_dt,
       Source_dt,
       Row_Source)
      SELECT res_cd,
             res_nm,
             Cmpnt_Matl_type_cd,
             Cmpnt_Mfg_part_num,
             Pdcd_Matl_type_cd,
             Pdcd_Mfg_part_num,
             Vnd_num,
             bom_status_cd vnd_qual_stat,
             cmpnt_site_num frm_site_num,
             frm_eng_site_num,
             to_eng_site_num,
             pkg_pct orig_alloc_pct,
             case
               when Active_flg = 'Y' then
                pkg_pct
               else
                0
             end Eng_Alloc_pct,
             case
               when eff_start_dt < SCMDATA.Dates.get_rpt_dt then
                SCMDATA.Dates.get_rpt_dt
               else
                eff_start_dt
             end Alloc_start_dt,
             case
               when eff_end_dt is null then
                SCMDATA.Dates.get_rpt_dt('HORIZON_END')
               else
                eff_end_dt
             end Alloc_end_dt,
             Active_flg,
             enterprise,
             updt_dt etl_dt,
             sysdate source_dt,
             'AVA-01'
        from (select distinct vas.*,
                              bom.pdcd_mfg_part_num,
                              bom.pdcd_matl_type_cd,
                              bom.pdcd_site_num,
                              bom.bom_status_cd,
                              bom.cmpnt_mfg_part_num,
                              bom.cmpnt_matl_type_cd,
                              smr.eng_site_num to_eng_site_num,
                              eer.enterprise,
                              case
                                when bom_status_cd in (2, 3) then
                                 'Y'
                                else
                                 'N'
                              end Active_flg,
                              smrb.eng_site_num frm_eng_site_num,
                              bom.cmpnt_site_num
                from (select snp.*,
                             LEAD(snp.eff_start_dt, 1) over(partition by snp.res_nm, snp.mfg_part_num, snp.vnd_num order by snp.eff_start_dt) eff_end_dt
                        from (select distinct res_cd,
                                              res_nm,
                                              mfg_part_num,
                                              vnd_num,
                                              eff_start_dt,
                                              pkg_pct,
                                              updt_dt
                                from VENDOR_ALLOCATION_SNP) snp) vas,
                     bom_master_src bom,
                     ref_data.site_master_ref smr,
                     ref_data.site_master_ref smrb,
                     ref_data.eng_enterprise_ref eer
               where eer.enterprise = i_enterprise_nm
                 and vas.vnd_num = smr.site_num
                 and smr.plannable_flg = 'Y'
                 and bom.cmpnt_site_num = smrb.site_num
                 and smrb.plannable_flg = 'Y'
                 and bom.cmpnt_site_num <> bom.pdcd_site_num --Added by SA on 12/20 to exclude intersite locations
                 and vas.mfg_part_num = bom.pdcd_mfg_part_num
                 and vas.vnd_num = bom.pdcd_site_num
                 and bom.pdcd_matl_type_cd in ('ZASY', 'ZBMP')
                 AND eer.ent_res_flg like '%:' || vas.res_cd || ':%' --addedd by SA on 1/11
                 and bom.cmpnt_matl_type_cd = 'ZSRT');
    commit;
  END;

  --********************************************************************
  -- TO load data into  BOD_DTL_TP_SRC table and the interim table
  -- This procedure is used to load vendor allocation data that later populates the bods
  --*********************************************************************
  PROCEDURE load_bod_dtl_tp_src(i_enterprise_nm IN VARCHAR2) IS
  
    -- where pdcd_mfg_part_num = 'MAX11800_TC+_A1'--'MAX5715A_UD+_A1'
    --        and cmpnt_mfg_part_num = 'FP08Y_S1' -- not 1
    -- pdcd_mfg_part_num = 'MAX5715A_UD+_A1' and cmpnt_mfg_part_num = 'DB51A-0B_S1';
  
    CURSOR cur_boddtltp_src IS
      select distinct pdcd_mfg_part_num, cmpnt_mfg_part_num, frm_site_num
        from Assy_Vnd_alloc_interm
       where enterprise = i_enterprise_nm
         and enterprise not like 'FP%'; ---Added by SA 01/11/18
  
    curval_boddtltp_src cur_boddtltp_src%ROWTYPE;
  
    ld_minstart_dt   DATE;
    ld_maxstart_dt   DATE;
    ld_minend_dt     DATE;
    ld_maxend_dt     DATE;
    ld_astart_dt     DATE;
    ld_aend_dt       DATE;
    ln_avail_rows    NUMBER(2);
    ln_sum_alloc_pct NUMBER := 0;
    ln_rem           NUMBER := 0;
  
    lv_row_source VARCHAR2(100) := NULL;
  
    -- Private procedure to populate the interim table
    PROCEDURE INSERT_Assy_Vndalloc_interm(i_enterprise_nm IN VARCHAR2) IS
    BEGIN
    
      frmdata.logs.begin_log('Start Assy_Vnd_alloc_interm load ');
      frmdata.delete_data('ASSY_VND_ALLOC_INTERM',
                          'enterprise = ' || '''' || i_enterprise_nm || '''',
                          10000);
    
      INSERT INTO Assy_Vnd_alloc_interm
        (ENTERPRISE,
         ALLOC_START_DT,
         ALLOC_END_DT,
         PDCD_MFG_PART_NUM,
         CMPNT_MFG_PART_NUM,
         frm_site_num, -- renamed
         cmpnt_site_num, --   renamed
         VND_NUM,
         pdcd_site_num,
         ENG_ALLOC_PCT,
         ETL_DT,
         TRANSIT_HRS,
         prio,
         SOURCE_DT)
        SELECT assy_vnd_alloc.                   enterprise,
               assy_vnd_alloc.alloc_start_dt,
               assy_vnd_alloc.alloc_end_dt,
               assy_vnd_alloc.                   pdcd_mfg_part_num,
               assy_vnd_alloc.cmpnt_mfg_part_num,
               assy_vnd_alloc.                   frm_site_num, -- renamed
               assy_vnd_alloc.cmpnt_site_num, --   renamed
               assy_vnd_alloc.vnd_num,
               assy_vnd_alloc.pdcd_site_num,
               assy_vnd_alloc.eng_alloc_pct,
               assy_vnd_alloc.etl_dt,
               bdm.transit_hrs,
               bdm.prio,
               sysdate                           source_dt
          from (select a.start_dt           alloc_start_dt,
                       a.end_dt             alloc_end_dt,
                       a.pdcd_mfg_part_num,
                       a.CMPNT_MFG_PART_NUM,
                       a.frm_site_num,
                       b.cmpnt_site_num,
                       b.vnd_num,
                       b.pdcd_site_num,
                       b.eng_alloc_pct,
                       b.etl_dt,
                       b.enterprise
                
                /*  b.VND_NUM,
                b.ORIG_ALLOC_PCT,
                SUM(b.ORIG_ALLOC_PCT) OVER(PARTITION BY a.pdcd_mfg_part_num, a.CMPNT_MFG_PART_NUM, a.frm_site_num, a.start_dt, a.end_dt) sum_ORIG_ALLOC_PCT*/
                  from (SELECT a.start_dt,
                               LEAD(a.start_dt) OVER(partition by a.pdcd_mfg_part_num, a.frm_site_num, a.CMPNT_MFG_PART_NUM order by a.pdcd_mfg_part_num, a.CMPNT_MFG_PART_NUM, a.frm_site_num, a.start_dt) end_dt,
                               a.pdcd_mfg_part_num,
                               a.CMPNT_MFG_PART_NUM,
                               a.frm_site_num
                          FROM (select DISTINCT start_dt,
                                                pdcd_mfg_part_num,
                                                frm_site_num,
                                                CMPNT_MFG_PART_NUM,
                                                cmpnt_site,
                                                ORIG_ALLOC_PCT
                                  from (select ALLOC_START_DT,
                                               ALLOC_END_DT,
                                               --trunc(sysdate) source_dt,
                                               pdcd_mfg_part_num,
                                               frm_site_num,
                                               CMPNT_MFG_PART_NUM,
                                               VND_NUM cmpnt_site,
                                               ORIG_ALLOC_PCT
                                          from Assy_Vnd_Alloc_Src
                                         where enterprise = i_enterprise_nm) unpivot(start_dt FOR value_type IN(ALLOC_START_DT,
                                                                                                                --  source_dt,
                                                                                                                ALLOC_END_DT))) a) a,
                       Assy_Vnd_Alloc_Src b
                 WHERE a.pdcd_mfg_part_num = b.pdcd_mfg_part_num
                   AND a.frm_site_num = b.frm_site_num
                   AND a.CMPNT_MFG_PART_NUM = b.CMPNT_MFG_PART_NUM
                   AND b.enterprise = i_enterprise_nm
                   AND a.start_dt <>
                       NVL(a.end_dt, to_date('12/30/2999', 'MM/DD/YYYY'))
                   AND a.start_dt >= b.ALLOC_START_DT
                   AND a.end_dt <= b.ALLOC_END_DT) assy_vnd_alloc,
               bod_master_src bdm
         where bdm.cmpnt_mfg_part_num = assy_vnd_alloc.cmpnt_mfg_part_num
           and bdm.cmpnt_site_num = assy_vnd_alloc.cmpnt_site_num
           and bdm.pdcd_site_num = assy_vnd_alloc.vnd_num;
    
      commit;
    
    END;
  
  BEGIN
  
    -- Populate the Assy_Vnd_alloc_interm table first
  
    INSERT_Assy_Vndalloc_interm(i_enterprise_nm);
    -- BOD_NAME should have <Assy_vnd_alloc_src.cmpnt_Mfg_part_num>_< Assy_vnd_alloc_src.frm_eng_site_num>_< Assy_vnd_alloc_src.to_eng_site_num>
  
    frmdata.logs.begin_log('Start BOD_DTL_TP_SRC load from SNP to SRC');
    --  frmdata.delete_data('RES_CAP_SRC', NULL, NULL);
    frmdata.delete_data('BOD_DTL_TP_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    lv_row_source := 'BODTP001';
  
    FOR curval_boddtltp_src IN cur_boddtltp_src LOOP
    
      ln_sum_alloc_pct := 0;
      ln_rem           := 0;
    
      select greatest(min(alloc_start_dt), SCMDATA.Dates.get_rpt_dt),
             --min(alloc_start_dt), SCMDATA.Dates.get_rpt_dt,SCMDATA.Dates.get_rpt_dt('HORIZON_END'),
             min(alloc_end_dt),
             max(alloc_start_dt),
             least(max(alloc_end_dt),
                   SCMDATA.Dates.get_rpt_dt('HORIZON_END'))
        into ld_minstart_dt, ld_minend_dt, ld_maxstart_dt, ld_maxend_dt
        from Assy_Vnd_alloc_interm
       where pdcd_mfg_part_num = curval_boddtltp_src.pdcd_mfg_part_num
         and cmpnt_mfg_part_num = curval_boddtltp_src.cmpnt_mfg_part_num
         and frm_site_num = curval_boddtltp_src.frm_site_num
         and enterprise = i_enterprise_nm;
    
      ld_astart_dt := ld_minstart_dt;
      ld_aend_dt   := ld_minend_dt;
    
      -- check if sum is equal to one between start date and end date
      select sum(eng_alloc_pct)
        into ln_sum_alloc_pct
        from Assy_Vnd_alloc_interm
       where pdcd_mfg_part_num = curval_boddtltp_src.pdcd_mfg_part_num
         and cmpnt_mfg_part_num = curval_boddtltp_src.cmpnt_mfg_part_num
         and frm_site_num = curval_boddtltp_src.frm_site_num
         and enterprise = i_enterprise_nm
         and alloc_start_dt >= ld_astart_dt
         and alloc_end_dt <= ld_aend_dt;
    
      if ln_sum_alloc_pct != 1 then
        ln_rem := 1 - ln_sum_alloc_pct;
      end if;
    
      /* dbms_output.put_line('ln_sum_alloc_pct is ' || ln_sum_alloc_pct ||
      'ln_rem  is ' || ln_rem);*/
    
      if ld_minstart_dt = ld_maxstart_dt and ld_minend_dt = ld_maxend_dt then
        -- go only once
      
        --   If less than or more than 1 adjust as per weighted ratios
      
        /* dbms_output.put_line(' inserting rows for ' || ld_astart_dt ||
        'and ' || ld_aend_dt);*/
      
        -- BOD_NAME should have <Assy_vnd_alloc_src.cmpnt_Mfg_part_num>_< Assy_vnd_alloc_src.frm_eng_site_num>_< Assy_vnd_alloc_src.to_eng_site_num>
      
        INSERT INTO BOD_DTL_TP_SRC
          (Eng_Bod_nm,
           PDCD_MFG_PART_NUM,
           cmpnt_mfg_part_num,
           cmpnt_site_num,
           pdcd_site_num,
           Alloc_pct,
           Eff_start_dt,
           Eff_end_dt,
           Transit_time,
           Transit_time_uom,
           Prio,
           Enterprise,
           Etl_Dt,
           Source_Dt,
           Row_Source)
          SELECT eng_bod_nm,
                 pdcd_Mfg_part_num,
                 cmpnt_mfg_part_num,
                 cmpnt_site_num,
                 pdcd_site_num,
                 Alloc_pct,
                 eff_start_dt,
                 eff_end_dt,
                 transit_time,
                 Transit_time_uom,
                 CASE
                   WHEN Alloc_pct IS NOT NULL AND Alloc_pct < 1 THEN
                    NULL
                   ELSE
                    prio
                 END prio,
                 enterprise,
                 etl_dt,
                 sysdate,
                 'BDP-01'
            FROM (select cmpnt_Mfg_part_num || '_' || cmpnt_site_num || '_' ||
                         pdcd_site_num as eng_bod_nm,
                         pdcd_Mfg_part_num,
                         cmpnt_mfg_part_num,
                         cmpnt_site_num,
                         pdcd_site_num,
                         sum(case
                               when ln_rem = 0 then
                                eng_alloc_pct
                               else
                                (ln_rem * (eng_alloc_pct / ln_sum_alloc_pct) +
                                eng_alloc_pct)
                             end) Alloc_pct,
                         ld_astart_dt eff_start_dt,
                         ld_aend_dt eff_end_dt,
                         max(transit_hrs) transit_time,
                         'HOUR' Transit_time_uom,
                         prio,
                         enterprise,
                         etl_dt,
                         sysdate,
                         lv_row_source
                    from Assy_Vnd_alloc_interm
                   where pdcd_mfg_part_num =
                         curval_boddtltp_src.pdcd_mfg_part_num
                     and cmpnt_mfg_part_num =
                         curval_boddtltp_src.cmpnt_mfg_part_num
                     and frm_site_num = curval_boddtltp_src.frm_site_num
                     and enterprise = i_enterprise_nm
                     and alloc_start_dt >= ld_astart_dt
                     and alloc_end_dt <= ld_aend_dt
                   group by cmpnt_Mfg_part_num || '_' || cmpnt_site_num || '_' ||
                            pdcd_site_num,
                            pdcd_Mfg_part_num,
                            cmpnt_mfg_part_num,
                            cmpnt_site_num,
                            pdcd_site_num,
                            ld_astart_dt,
                            ld_aend_dt,
                            prio,
                            enterprise,
                            etl_dt,
                            sysdate,
                            lv_row_source);
      
        commit;
      
      else
      
        /*dbms_output.put_line(' inserting rows for ' || ld_astart_dt ||
        'and ' || ld_aend_dt);*/
      
        INSERT INTO BOD_DTL_TP_SRC
          (Eng_Bod_nm,
           PDCD_MFG_PART_NUM,
           cmpnt_mfg_part_num,
           cmpnt_site_num,
           pdcd_site_num,
           Alloc_pct,
           Eff_start_dt,
           Eff_end_dt,
           Transit_time,
           Transit_time_uom,
           Prio,
           Enterprise,
           Etl_Dt,
           Source_Dt,
           Row_Source)
          SELECT eng_bod_nm,
                 pdcd_Mfg_part_num,
                 cmpnt_mfg_part_num,
                 cmpnt_site_num,
                 pdcd_site_num,
                 Alloc_pct,
                 eff_start_dt,
                 eff_end_dt,
                 transit_time,
                 Transit_time_uom,
                 CASE
                   WHEN Alloc_pct IS NOT NULL AND Alloc_pct < 1 THEN
                    NULL
                   ELSE
                    prio
                 END prio,
                 enterprise,
                 etl_dt,
                 sysdate,
                 'BDP-02'
            FROM (select cmpnt_Mfg_part_num || '_' || cmpnt_site_num || '_' ||
                         pdcd_site_num as eng_bod_nm,
                         pdcd_Mfg_part_num,
                         cmpnt_mfg_part_num,
                         cmpnt_site_num,
                         pdcd_site_num,
                         sum(case
                               when ln_rem = 0 then
                                eng_alloc_pct
                               else
                                (ln_rem * (eng_alloc_pct / ln_sum_alloc_pct) +
                                eng_alloc_pct)
                             end) Alloc_pct,
                         ld_astart_dt eff_start_dt,
                         ld_aend_dt eff_end_dt,
                         max(transit_hrs) transit_time,
                         'HOUR' Transit_time_uom,
                         prio,
                         enterprise,
                         etl_dt,
                         sysdate,
                         lv_row_source
                    from Assy_Vnd_alloc_interm
                   where pdcd_mfg_part_num =
                         curval_boddtltp_src.pdcd_mfg_part_num
                     and cmpnt_mfg_part_num =
                         curval_boddtltp_src.cmpnt_mfg_part_num
                     and frm_site_num = curval_boddtltp_src.frm_site_num
                     and enterprise = i_enterprise_nm
                     and alloc_start_dt >= ld_astart_dt
                     and alloc_end_dt <= ld_aend_dt
                   group by cmpnt_Mfg_part_num || '_' || cmpnt_site_num || '_' ||
                            pdcd_site_num,
                            pdcd_Mfg_part_num,
                            cmpnt_mfg_part_num,
                            cmpnt_site_num,
                            pdcd_site_num,
                            ld_astart_dt,
                            ld_aend_dt,
                            prio,
                            enterprise,
                            etl_dt,
                            sysdate,
                            lv_row_source);
      
        -- 2nd set onwards
      
        ld_astart_dt := ld_minend_dt;
        select least(min(alloc_end_dt),
                     SCMDATA.Dates.get_rpt_dt('HORIZON_END'))
          into ld_aend_dt
          from Assy_Vnd_alloc_interm
         where pdcd_mfg_part_num = curval_boddtltp_src.pdcd_mfg_part_num
           and cmpnt_mfg_part_num = curval_boddtltp_src.cmpnt_mfg_part_num
           and frm_site_num = curval_boddtltp_src.frm_site_num
           and enterprise = i_enterprise_nm
           and alloc_start_dt >= ld_astart_dt;
      
        select count(*)
          into ln_avail_rows --frm_site_num,pdcd_site_num,eng_alloc_pct
          from Assy_Vnd_alloc_interm
         where pdcd_mfg_part_num = curval_boddtltp_src.pdcd_mfg_part_num --'MAX5715A_UD+_A1'
           and cmpnt_mfg_part_num = curval_boddtltp_src.cmpnt_mfg_part_num --'DB51A-0B_S1'
           and frm_site_num = curval_boddtltp_src.frm_site_num -- '4900'
           and enterprise = i_enterprise_nm
           and alloc_start_dt >= ld_astart_dt
           and alloc_end_dt <= ld_aend_dt;
      
        while ln_avail_rows > 0 -- if condition true proceed otherwise end
        
         LOOP
          /*
          dbms_output.put_line('in while loop');
          
          dbms_output.put_line('ln_avail_rows is ' || ln_avail_rows);
          dbms_output.put_line(' inserting rows for ' || ld_astart_dt ||
                               'and ' || ld_aend_dt);*/
        
          ln_sum_alloc_pct := 0;
          ln_rem           := 0;
        
          -- check if sum is equal to one between start date and end date
          select sum(eng_alloc_pct)
            into ln_sum_alloc_pct
            from Assy_Vnd_alloc_interm
           where pdcd_mfg_part_num = curval_boddtltp_src.pdcd_mfg_part_num
             and cmpnt_mfg_part_num =
                 curval_boddtltp_src.cmpnt_mfg_part_num
             and frm_site_num = curval_boddtltp_src.frm_site_num
             and enterprise = i_enterprise_nm
             and alloc_start_dt >= ld_astart_dt
             and alloc_end_dt <= ld_aend_dt;
        
          if ln_sum_alloc_pct != 1 then
            ln_rem := 1 - ln_sum_alloc_pct;
          end if;
        
          /*   dbms_output.put_line(' ln_sum_alloc_pct is ' || ln_sum_alloc_pct ||
          '  ln_rem  is ' || ln_rem);*/
        
          INSERT INTO BOD_DTL_TP_SRC
            (Eng_Bod_nm,
             PDCD_MFG_PART_NUM,
             cmpnt_mfg_part_num,
             cmpnt_site_num,
             pdcd_site_num,
             Alloc_pct,
             Eff_start_dt,
             Eff_end_dt,
             Transit_time,
             Transit_time_uom,
             Prio,
             Enterprise,
             Etl_Dt,
             Source_Dt,
             Row_Source)
            SELECT eng_bod_nm,
                   pdcd_Mfg_part_num,
                   cmpnt_mfg_part_num,
                   cmpnt_site_num,
                   pdcd_site_num,
                   Alloc_pct,
                   eff_start_dt,
                   eff_end_dt,
                   transit_time,
                   Transit_time_uom,
                   CASE
                     WHEN Alloc_pct IS NOT NULL AND Alloc_pct < 1 THEN
                      NULL
                     ELSE
                      prio
                   END prio,
                   enterprise,
                   etl_dt,
                   sysdate,
                   'BDP-03'
              FROM (select cmpnt_Mfg_part_num || '_' || cmpnt_site_num || '_' ||
                           pdcd_site_num as eng_bod_nm,
                           pdcd_Mfg_part_num,
                           cmpnt_mfg_part_num,
                           cmpnt_site_num,
                           pdcd_site_num,
                           sum(case
                                 when ln_rem = 0 then
                                  eng_alloc_pct
                                 else
                                  (ln_rem * (eng_alloc_pct / ln_sum_alloc_pct) +
                                  eng_alloc_pct)
                               end) Alloc_pct,
                           -- to_date(ld_astart_dt, 'MM/DD/YYYY') eff_start_dt,
                           -- to_date(ld_aend_dt, 'MM/DD/YYYY') eff_end_dt,
                           ld_astart_dt eff_start_dt,
                           ld_aend_dt eff_end_dt,
                           max(transit_hrs) transit_time,
                           'HOUR' Transit_time_uom,
                           prio,
                           enterprise,
                           etl_dt,
                           sysdate,
                           lv_row_source
                    --to_date('11/3/2017','MM/DD/YYYY') eff_start_dt, to_date('1/16/2018','MM/DD/YYYY') eff_end_dt
                      from Assy_Vnd_alloc_interm
                     where pdcd_mfg_part_num =
                           curval_boddtltp_src.pdcd_mfg_part_num
                       and cmpnt_mfg_part_num =
                           curval_boddtltp_src.cmpnt_mfg_part_num
                       and frm_site_num = curval_boddtltp_src.frm_site_num
                       and enterprise = i_enterprise_nm
                       and alloc_start_dt >= ld_astart_dt
                       and alloc_end_dt <= ld_aend_dt
                     group by cmpnt_Mfg_part_num || '_' || cmpnt_site_num || '_' ||
                              pdcd_site_num,
                              pdcd_Mfg_part_num,
                              cmpnt_mfg_part_num,
                              cmpnt_site_num,
                              pdcd_site_num,
                              ld_astart_dt,
                              ld_aend_dt,
                              enterprise,
                              etl_dt,
                              prio,
                              sysdate,
                              lv_row_source);
        
          ld_astart_dt := ld_aend_dt;
          select least(min(alloc_end_dt),
                       SCMDATA.Dates.get_rpt_dt('HORIZON_END'))
            into ld_aend_dt
            from Assy_Vnd_alloc_interm
           where pdcd_mfg_part_num = curval_boddtltp_src.pdcd_mfg_part_num
             and cmpnt_mfg_part_num =
                 curval_boddtltp_src.cmpnt_mfg_part_num
             and frm_site_num = curval_boddtltp_src.frm_site_num
             and enterprise = i_enterprise_nm
             and alloc_start_dt >= ld_astart_dt;
        
          /*dbms_output.put_line(' now start dt and end date is ' ||
          ld_astart_dt || 'and ' || ld_aend_dt);*/
        
          select count(*)
            into ln_avail_rows --frm_site_num,pdcd_site_num,eng_alloc_pct
            from Assy_Vnd_alloc_interm
           where pdcd_mfg_part_num = curval_boddtltp_src.pdcd_mfg_part_num --'MAX5715A_UD+_A1'
             and cmpnt_mfg_part_num =
                 curval_boddtltp_src.cmpnt_mfg_part_num --'DB51A-0B_S1'
             and frm_site_num = curval_boddtltp_src.frm_site_num -- '4900'
             and enterprise = i_enterprise_nm
             and alloc_start_dt >= ld_astart_dt --to_date('2/2/2019','MM/DD/YYYY'); --  ld_aend_dt ;
             and alloc_end_dt <= ld_aend_dt;
        
        END LOOP;
      
      end if;
    
    END LOOP;
    --remove later
    delete BOD_DTL_TP_SRC
     where enterprise = i_enterprise_nm
       and eng_bod_nm in
           (select eng_bod_nm
              from (select bodtp.eng_bod_nm,
                           bodtp.cmpnt_site_num,
                           bodtp.pdcd_site_num,
                           bodtp.cmpnt_mfg_part_num,
                           bodtp.eff_start_dt,
                           bodtp.enterprise
                      from BOD_DTL_TP_SRC bodtp
                     WHERE enterprise = i_enterprise_nm
                       AND bodtp.cmpnt_site_num <> bodtp.pdcd_site_num --Added by SA on 12/20 excluding intersite locations
                       AND enterprise NOT LIKE '%FP%'
                     group by bodtp.eng_bod_nm,
                              bodtp.cmpnt_site_num,
                              bodtp.pdcd_site_num,
                              bodtp.cmpnt_mfg_part_num,
                              bodtp.eff_start_dt,
                              bodtp.enterprise
                    having count(*) > 1));
    commit;
  END;

  --************************************************************
  --*  Procedures to load data from SRC to ODS tables *
  --************************************************************

  --************************************************************
  --Procedure to load data into RESOURCEMASTETR table --1
  --  This procedure is used to load data for Sort and Test, Assembly, Fab resources
  --************************************************************
  PROCEDURE load_RESOURCEMASTER(i_enterprise_nm IN VARCHAR2) IS
  
    -- lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date
  
  BEGIN
  
    frmdata.logs.begin_log('Start RESOURCEMASTER load from SRC to ODS');
    --  frmdata.delete_data('RESOURCEMASTER', NULL, NULL);
    frmdata.delete_data('RESOURCEMASTER_STG',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    INSERT INTO scmdata.resourcemaster_stg
      SELECT DISTINCT eer.enterprise, --19294
                      'RES_SITE' as SITEID,
                      rcs.eng_res_nm,
                      case
                        when i_enterprise_nm like 'OP%' then
                         null
                        else
                         'C'
                      end as RESOURCECLASS,
                      'UNIT' as RESOURCETYPE,
                      eer.engine_id,
                      sysdate
        FROM ref_data.eng_enterprise_ref eer, scmdata.RES_CAP_SRC rcs
       WHERE CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = rcs.enterprise
         AND eer.enterprise = i_enterprise_nm;
  
    ---FP
    INSERT INTO resourcemaster_stg
      (engine_id, enterprise, resourcename, siteid, sourcedate)
      SELECT eer.engine_id,
             eer.enterprise,
             'DUMMY',
             eer.enterprise siteid,
             TRUNC(SYSDATE)
        from ref_data.eng_enterprise_ref eer
       where enterprise = i_enterprise_nm
         AND eer.enterprise like 'FP_MRP%';
  
    Commit; /* remove later  */
    frmdata.logs.info('RESOURCEMASTER_STG load from SRC to ODS is completed');
  
  END load_RESOURCEMASTER;

  --************************************************************
  --Procedure to load data into RESOURCECALENDAR table --2
  --  This procedure is used to load data for Sort and Test, Assembly, Fab resources
  --************************************************************
  PROCEDURE load_RESOURCECALENDAR(i_enterprise_nm IN VARCHAR2) IS
  
    -- lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date
  
  BEGIN
  
    frmdata.logs.begin_log('Start RESOURCECALENDAR_STG load from SRC TO ODS');
    --  frmdata.delete_data('RESOURCECALENDAR_STG', NULL, NULL);
    frmdata.delete_data('RESOURCECALENDAR_STG',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
    IF i_enterprise_nm = 'SCP_STARTS' ----Added by VS 10/17
     THEN
      INSERT INTO scmdata.RESOURCECALENDAR_STG
        SELECT DISTINCT eer.enterprise, --19294
                        'RES_SITE' as SITEID,
                        rcs.eng_res_nm as resourcename,
                        rcs.eng_res_nm as workcentername,
                        'CAPACITY' as CALENDERTYPE,
                        rcs.eng_res_nm as CALENDERNAME,
                        eer.engine_id,
                        sysdate
          FROM ref_data.eng_enterprise_ref eer, scmdata.RES_CAP_SRC rcs
         Where eer.enterprise = rcs.enterprise
           AND eer.enterprise = i_enterprise_nm;
    else
      INSERT INTO scmdata.RESOURCECALENDAR_STG
        SELECT DISTINCT eer.enterprise, --19294
                        'RES_SITE' as SITEID,
                        rcs.eng_res_nm as resourcename,
                        rcs.eng_res_nm as workcentername,
                        'CAPACITY' as CALENDERTYPE,
                        rcs.eng_res_nm as CALENDERNAME,
                        eer.engine_id,
                        sysdate
          FROM ref_data.eng_enterprise_ref eer, scmdata.RES_CAP_SRC rcs
         Where eer.enterprise = rcs.enterprise
           AND eer.enterprise = i_enterprise_nm
           and eer.enterprise not like 'OP%';
    end if;
  
    Commit; /* remove later  */
    frmdata.logs.info('RESOURCECALENDAR_STG load from SRC TO ODS is completed');
  
  END load_RESOURCECALENDAR;

  --************************************************************
  --Procedure to load data into WORKCENTERMASTER table --3
  --  This procedure is used to load data for Sort and Test, Assembly, Fab resources
  --************************************************************
  PROCEDURE load_WORKCENTERMASTER(i_enterprise_nm IN VARCHAR2) IS
  
    -- lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date
  
  BEGIN
  
    frmdata.logs.begin_log('Start load_WORKCENTERMASTER load from SRC to ODS');
    -- frmdata.delete_data('WORKCENTERMASTER_STG', NULL, NULL);
    frmdata.delete_data('WORKCENTERMASTER_STG',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    INSERT INTO scmdata.workcentermaster_stg
      (enterprise, siteid, workcentername, category, engine_id, sourcedate)
      (SELECT DISTINCT eer.enterprise, --19294
                       'RES_SITE' as SITEID,
                       rcs.eng_res_nm as workcentername,
                       'MANUFACTURING' as CATEGORY,
                       eer.engine_id,
                       sysdate
         FROM ref_data.eng_enterprise_ref eer, scmdata.RES_CAP_SRC rcs
        WHERE CASE
                WHEN i_enterprise_nm LIKE 'OP%' THEN
                 'SCP_DAILY'
                ELSE
                 eer.enterprise
              END = rcs.enterprise
          AND eer.enterprise = i_enterprise_nm);
    --------FP------------
    INSERT INTO workcentermaster_stg
      (enterprise, siteid, workcentername, engine_id, sourcedate)
      SELECT eer.enterprise,
             eer.enterprise siteid,
             'DUMMY',
             eer.engine_id,
             SYSDATE
        from ref_data.eng_enterprise_ref eer
       where enterprise = i_enterprise_nm
         AND eer.enterprise like 'FP_MRP%';
  
    Commit; /* remove later  */
    frmdata.logs.info('load_WORKCENTERMASTER_STG load from SRC to ODS is completed');
  
  END load_WORKCENTERMASTER;

  --************************************************************
  --Procedure to load data into WORKCENTERDETAIL table --4
  --  This procedure is used to load data for Sort and Test, Assembly, Fab resources
  --************************************************************
  PROCEDURE load_WORKCENTERDETAIL(i_enterprise_nm IN VARCHAR2) IS
  
    --  lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
  
  BEGIN
  
    frmdata.logs.begin_log('Start load_WORKCENTERDETAIL load from SRC to ODS');
    --frmdata.delete_data('WORKCENTERDETAIL_STG', NULL, NULL);
    frmdata.delete_data('WORKCENTERDETAIL_STG',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    INSERT INTO scmdata.workcenterdetail_stg
      SELECT DISTINCT eer.enterprise, --19294
                      'RES_SITE' as SITEID,
                      rcs.eng_res_nm as workcenterename,
                      rcs.eng_res_nm as resourcename,
                      'FLOW_LIMIT_CALENDAR' as LOADPOLICY,
                      '1' as IS_RESPONSE_BUFFER,
                      rcs.res_area as STAGE,
                      eer.engine_id,
                      sysdate,
                      NULL,
                      NULL,
                      NULL,
                      NULL
        FROM ref_data.eng_enterprise_ref eer, scmdata.RES_CAP_SRC rcs
       WHERE CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = rcs.enterprise
         AND eer.ent_res_flg like '%:' || rcs.RES_AREA || ':%' --added by SA on 12/22
         AND eer.enterprise = i_enterprise_nm;
    ---------FP-----------
  
    INSERT INTO workcenterdetail_stg
      (enterprise,
       siteid,
       workcentername,
       resourcename,
       engine_id,
       sourcedate,
       locationid)
      SELECT eer.enterprise,
             eer.enterprise siteid,
             'DUMMY',
             'DUMMY',
             eer.engine_id,
             TRUNC(SYSDATE),
             'DUMMY'
        from ref_data.eng_enterprise_ref eer
       where enterprise = i_enterprise_nm
         AND eer.enterprise like 'FP_MRP%';
  
    Commit; /* remove later */
    frmdata.logs.info('WORKCENTERDETAIL_STG load from SRC to ODS is completed');
  
  END load_WORKCENTERDETAIL;
  --************************************************************
  --Procedure to load data into CALENDARMASTER table --5
  --  This procedure is used to load data for Sort and Test, Assembly, Fab resources
  --************************************************************
  PROCEDURE load_CALENDARMASTER(i_enterprise_nm IN VARCHAR2) IS
  
    --  lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
  
    -- fixing duplicates issues
  
  BEGIN
    frmdata.logs.begin_log('Start load_CALENDARMASTER load from SRC to ODS');
    -- frmdata.delete_data('CALENDARMASTER_STG', NULL, NULL);
  
    /*  DELETE FROM CALENDARMASTER_STG
    WHERE CALENDARNAME
    
     WHERE EXISTS (SELECT NULL
              FROM resourcemaster_stg rm
             WHERE rm.resourcename = op.RESOURCENAME --560 of 560 rows
            );*/
  
    IF i_enterprise_nm = 'SCP_STARTS' ----Added by VS 10/17
     THEN
      INSERT INTO scmdata.calendarmaster_stg
        SELECT DISTINCT rcs.eng_res_nm,
                        NULL,
                        eer.engine_id,
                        eer.enterprise,
                        SYSDATE
          FROM ref_data.eng_enterprise_ref eer, res_cap_src rcs
         WHERE eer.enterprise = rcs.enterprise
           AND eer.enterprise = i_enterprise_nm;
    ELSE
      INSERT INTO scmdata.calendarmaster_stg
        SELECT DISTINCT rcs.eng_res_nm as Calendarname,
                        NULL           as Description,
                        eer.engine_id,
                        eer.enterprise,
                        sysdate
          FROM ref_data.eng_enterprise_ref eer, scmdata.RES_CAP_SRC rcs
         WHERE eer.enterprise = rcs.enterprise
           AND eer.enterprise = i_enterprise_nm
           and eer.enterprise not like 'OP%';
    END IF;
    Commit; /* remove later */
    frmdata.logs.info('CALENDARMASTER_STG load from SRC to ODS is completed');
  
  END load_CALENDARMASTER;
  --************************************************************
  --Procedure to load data into CALENDARDETAIL table --6
  --  This procedure is used to load data for Sort and Test, Assembly, Fab resources
  --************************************************************
  PROCEDURE load_CALENDARDETAIL(i_enterprise_nm IN VARCHAR2) IS
  
    -- This cursor is used to load data for Sort and Test resources
    -- use resource_type ps_resource_name for FP
    -- or use lc_resource_name for SCP_DAILY or distinct when selecting from scmdata.RES_CAP_SRC
    -------same logic for ATE ------------
    CURSOR cur_caldetail IS
      SELECT /*+ parallel */
       b.*,
       CASE
         WHEN b.patternseq = 1 then
          (SELECT SUBSTR(TO_CHAR(p.currentdate, 'Dy'), 1, 2)
             FROM odsscp.planmaster p
            WHERE p.enterprise = i_enterprise_nm
              AND p.engine_id = b.engine_id) --curval_caldetail(i).engine_id)
         ELSE
          'Su'
       END attr_value
        FROM (SELECT dense_rank() over(partition by CalendarName order by effstartdate) as patternseq,
                     a.*
                FROM (SELECT distinct rcs.eng_res_nm as CalendarName,
                                      rcs.site_num as SiteID, --why site id is null in specs?
                                      NULL as Item_name,
                                      (SELECT v.WK_END_DT + 1
                                         FROM scmdata.DMCLS_SNP_VW v
                                        WHERE v.WK_NUM = rcs.wk_num
                                          AND v.FY = rcs.yr_num
                                          AND v.WK_end_YN = 'Y'
                                          and rownum > 0) as effenddate,
                                      (SELECT v.WK_BGN_DT
                                         FROM scmdata.DMCLS_SNP_VW v
                                        WHERE v.WK_NUM = rcs.wk_num
                                          AND v.FY = rcs.yr_num
                                          AND v.WK_bgn_YN = 'Y'
                                          and rownum > 0) as effstartdate,
                                      eer.engine_id,
                                      eer.enterprise,
                                      rcs.res_area,
                                      'days' ATTRIBUTE,
                                      'hours' VALUEUOM,
                                      /*CASE
                                        WHEN rcs.resource_area in
                                             ('ATE FT', 'ATE PBS', 'ATE WS') THEN
                                         'days'
                                      END ATTRIBUTE,*/
                                      /*CASE
                                        WHEN rcs.resource_area in
                                             ('ATE FT', 'ATE PBS', 'ATE WS') THEN
                                         'hours'
                                      END VALUEUOM,*/
                                      rcs.eff_res_cnt * 168 as VALUE --
                        FROM ref_data.eng_enterprise_ref eer,
                             scmdata.RES_CAP_SRC         rcs
                       WHERE eer.enterprise = rcs.enterprise
                         AND eer.enterprise = i_enterprise_nm
                            --   and eer.enterprise not like 'OP%'  --commented by SA on 12/22
                         AND eer.ent_res_flg like
                             '%:' || rcs.RES_AREA || ':%' --added by SA on 12/22
                         AND rcs.res_area not in
                             ('DIECOAT', 'NON ATE FT', 'PKG', 'BUMP')
                            -- AND rcs.resource_area in
                            --     ('ATE FT', 'ATE PBS', 'ATE WS')
                         AND rcs.res_nm != 'UNKNOWN'
                         and rownum > 0) a
               WHERE a.effstartdate >=
                     scmdata.engines.get_plan_currentdate(i_enterprise_nm)
                 and rownum > 0
               order by CalendarName, SiteID, effstartdate) b;
  
    TYPE curtab_caldetail IS TABLE OF cur_caldetail%ROWTYPE;
    curval_caldetail curtab_caldetail := curtab_caldetail();
    --curval_caldetail cur_caldetail%ROWTYPE;
  
    --------below cursor for NON ATE---------
  
    CURSOR cur_caldetail_nonate IS
      SELECT b.*,
             CASE
               WHEN b.patternseq = 1 then
                (SELECT SUBSTR(TO_CHAR(p.currentdate, 'Dy'), 1, 2)
                   FROM odsscp.planmaster p
                  WHERE p.enterprise = i_enterprise_nm
                    AND p.engine_id = b.engine_id)
               ELSE
                'Su'
             END attr_value
        FROM (SELECT dense_rank() over(partition by CalendarName order by effstartdate) as patternseq,
                     a.*
                FROM (SELECT distinct rcs.eng_res_nm as CalendarName,
                                      'RES_SITE' as SiteID,
                                      NULL as Item_name,
                                      (SELECT v.WK_END_DT + 1
                                         FROM scmdata.DMCLS_SNP_VW v
                                        WHERE v.WK_NUM = rcs.wk_num
                                          AND v.FY = rcs.yr_num
                                          AND v.WK_end_YN = 'Y'
                                          and rownum > 0) as effenddate,
                                      (SELECT v.WK_BGN_DT
                                         FROM scmdata.DMCLS_SNP_VW v
                                        WHERE v.WK_NUM = rcs.wk_num
                                          AND v.FY = rcs.yr_num
                                          AND v.WK_bgn_YN = 'Y'
                                          and rownum > 0) as effstartdate,
                                      eer.engine_id,
                                      eer.enterprise,
                                      rcs.res_area,
                                      'days' ATTRIBUTE,
                                      'hours' VALUEUOM,
                                      rcs.eff_res_cnt * 168 as VALUE --will be changed for NON ATE
                        FROM ref_data.eng_enterprise_ref eer,
                             scmdata.RES_CAP_SRC         rcs
                       WHERE eer.enterprise = rcs.enterprise
                         AND eer.enterprise = i_enterprise_nm
                         AND eer.ent_res_flg like
                             '%:' || rcs.RES_AREA || ':%' --added by SA on 12/22
                         AND rcs.res_area = 'NON ATE FT'
                         AND rcs.res_nm != 'UNKNOWN'
                         and rownum > 0) a
              /* WHERE a.effstartdate >=
              scmdata.engines.get_plan_currentdate(i_enterprise_nm)*/ ---removed it to get all the weeks data between plan current and horizonend
               where rownum > 0
               order by CalendarName, SiteID, effstartdate) b;
  
    TYPE curtab_caldetail_nonate IS TABLE OF cur_caldetail_nonate%ROWTYPE;
    curval_caldetail_nonate curtab_caldetail_nonate := curtab_caldetail_nonate();
  
    ------------ to populates data for SCP_STARTS----------
    CURSOR cur_cal IS
      SELECT DISTINCT b.*,
                      CASE
                        WHEN b.patternseq = 1 THEN
                         (SELECT SUBSTR(TO_CHAR(pm.currentdate, 'Dy'), 1, 2)
                            FROM odsscp.planmaster pm
                           WHERE pm.enterprise = i_enterprise_nm
                             AND pm.engine_id = b.engine_id)
                        ELSE
                         'Su'
                      END attr_value
        FROM (SELECT DISTINCT dense_rank() OVER(PARTITION BY calendarname, siteid ORDER BY effstartdate) patternseq,
                              a.*
                FROM (SELECT DISTINCT rcs.eng_res_nm calendarname,
                                      'RES_SITE' siteid,
                                      eer.engine_id,
                                      eer.enterprise,
                                      rcs.eff_res_cnt,
                                      SYSDATE sourcedate,
                                      (SELECT wk_end_dt + 1
                                         FROM dmcls_snp_vw cal
                                        WHERE cal.wk_num = rcs.wk_num
                                          AND cal.yr_num = rcs.yr_num
                                          AND wk_end_yn = 'Y'
                                          and rownum > 0) effenddate,
                                      (SELECT wk_bgn_dt
                                         FROM dmcls_snp_vw cal
                                        WHERE cal.wk_num = rcs.wk_num
                                          AND cal.yr_num = rcs.yr_num
                                          AND wk_bgn_yn = 'Y'
                                          and rownum > 0) effstartdate
                        FROM ref_data.eng_enterprise_ref eer, res_cap_src rcs
                       WHERE eer.enterprise = i_enterprise_nm
                            -- and eer.enterprise not like 'OP%'
                         AND eer.enterprise = rcs.enterprise
                         and rownum > 0) a
               WHERE a.effstartdate >=
                     scmdata.engines.get_plan_currentdate(i_enterprise_nm)
                 and rownum > 0
               ORDER BY calendarname, siteid, effstartdate) b;
  
    TYPE tab_cal IS TABLE OF cur_cal%ROWTYPE;
  
    curval_cal tab_cal := tab_cal();
  
    --------------------------------------------------------------------------------------
    ------------ to populates data for Assembly/Bump/Diecoat resources ----------
  
    ln_asmbly_days NUMBER(3);
    ld_rpt_startdt date;
    ld_rpt_enddt   date;
  
    CURSOR cur_caldetail_asmbly_dly IS
    --cur_res_capacity_mp IS
      SELECT DISTINCT dense_rank() OVER(PARTITION BY calendarname, siteid ORDER BY effstartdate) patternseq,
                      a.*
        from (select eng_res_nm calendarname,
                     siteid,
                     yr_num,
                     wk_num,
                     effstartdate,
                     effenddate,
                     sum(eff_res_cnt) as value,
                     valueuom,
                     engine_id,
                     enterprise
              --wstartdate,wenddate,--sum(eff_res_cnt)
              --select *
                from (select rcs.eng_res_nm,
                             rcs.site_num as siteid,
                             (SELECT v.cal_dt
                                FROM scmdata.DMCLS_SNP_VW v
                               WHERE v.WK_NUM = rcs.wk_num
                                 AND v.FY = rcs.yr_num
                                 AND v.day_of_wk = rcs.day_of_wk
                                 and rownum > 0) as effstartdate,
                             (SELECT v.cal_dt + 1
                                FROM scmdata.DMCLS_SNP_VW v
                               WHERE v.WK_NUM = rcs.wk_num
                                 AND v.FY = rcs.yr_num
                                 AND v.day_of_wk = rcs.day_of_wk
                                 and rownum > 0) as effenddate,
                             rcs.wk_num,
                             rcs.yr_num,
                             rcs.day_of_wk,
                             rcs.eff_res_cnt,
                             rcs.cap_uom VALUEUOM,
                             eer.engine_id,
                             eer.enterprise
                        FROM ref_data.eng_enterprise_ref eer,
                             scmdata.RES_CAP_SRC         rcs
                       WHERE eer.enterprise = rcs.enterprise
                         AND eer.enterprise = i_enterprise_nm
                         AND eer.ent_res_flg like
                             '%:' || rcs.RES_AREA || ':%' --added by SA on 12/22
                         AND rcs.res_area in ('PKG', 'BUMP', 'DIECOAT')
                         and rownum > 0
                      -- AND rcs.res_nm = 'T1433+2'
                      -- and rcs.site_num = '0007900011'
                      )
               where effstartdate > = ld_rpt_startdt
                 and effstartdate < ld_rpt_startdt + ln_asmbly_days
                 and rownum > 0
               group by eng_res_nm,
                        siteid,
                        yr_num,
                        wk_num,
                        effstartdate,
                        effenddate,
                        valueuom,
                        engine_id,
                        enterprise) a;
  
    CURSOR cur_caldetail_asmbly IS
      SELECT DISTINCT dense_rank() OVER(PARTITION BY calendarname, siteid ORDER BY effstartdate) + ln_asmbly_days patternseq,
                      a.*
        from (select eng_res_nm calendarname,
                     siteid,
                     yr_num,
                     wk_num,
                     wstartdate as effstartdate,
                     wenddate + 1 as effenddate,
                     sum(eff_res_cnt) as value,
                     valueuom,
                     engine_id,
                     enterprise
              --wstartdate,wenddate,--sum(eff_res_cnt)
              --select *
                from (select rcs.eng_res_nm,
                             rcs.site_num as siteid,
                             (SELECT v.cal_dt
                                FROM scmdata.DMCLS_SNP_VW v
                               WHERE v.WK_NUM = rcs.wk_num
                                 AND v.FY = rcs.yr_num
                                 AND v.day_of_wk = rcs.day_of_wk
                                 and rownum > 0) as effstartdate,
                             (SELECT v.WK_BGN_DT
                                FROM scmdata.DMCLS_SNP_VW v
                               WHERE v.WK_NUM = rcs.wk_num
                                 AND v.FY = rcs.yr_num
                                 AND v.day_of_wk = rcs.day_of_wk
                                 and rownum > 0) as wstartdate,
                             (SELECT v.WK_END_DT
                                FROM scmdata.DMCLS_SNP_VW v
                               WHERE v.WK_NUM = rcs.wk_num
                                 AND v.FY = rcs.yr_num
                                 AND v.day_of_wk = rcs.day_of_wk
                                 and rownum > 0) as wenddate,
                             rcs.wk_num,
                             rcs.yr_num,
                             rcs.day_of_wk,
                             rcs.eff_res_cnt,
                             rcs.cap_uom VALUEUOM,
                             eer.engine_id,
                             eer.enterprise
                        FROM ref_data.eng_enterprise_ref eer,
                             scmdata.RES_CAP_SRC         rcs
                       WHERE eer.enterprise = rcs.enterprise
                         AND eer.enterprise = i_enterprise_nm
                         AND eer.ent_res_flg like
                             '%:' || rcs.RES_AREA || ':%' --added by SA on 12/22
                         AND rcs.res_area in ('PKG', 'BUMP', 'DIECOAT') --=
                         and rownum > 0
                      --    AND rcs.res_nm = 'T1433+2'
                      --   and rcs.site_num = '0007900011'
                      )
               where effstartdate > = ld_rpt_startdt + ln_asmbly_days
                 and rownum > 0
                    /* (select bucketsize
                     from odsscp.Bucketpattern
                    where BUCKETSIZEUOM = 'day'
                      and BUCKETNAME = 'LP_HORIZON'
                      and enterprise = 'SCP_DAILY') */
                 and effstartdate <= ld_rpt_enddt
               group by eng_res_nm,
                        siteid,
                        yr_num,
                        wk_num,
                        wstartdate,
                        wenddate,
                        valueuom,
                        engine_id,
                        enterprise) a;
  
    TYPE curtab_caldetail_asmbly IS TABLE OF cur_caldetail_asmbly%ROWTYPE;
    curval_caldetail_asmbly curtab_caldetail_asmbly := curtab_caldetail_asmbly();
  
    curval_asmbly_dly cur_caldetail_asmbly_dly%ROWTYPE;
  
    ------------------------------------------------------------------------------------------------------------------------------------------
  
  BEGIN
    frmdata.logs.begin_log('Start load_CALENDARDETAIL load from SRC to ODS');
    --   frmdata.delete_data('CALENDARDETAIL_STG_1', NULL, NULL);
    --   frmdata.delete_data('CALBASEDATTR_STG_1', NULL, NULL);
    -- frmdata.delete_data('CALPATDETAIL_STG', NULL, NULL);
    frmdata.delete_data('CALPATDETAIL_STG',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
    -- lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
    IF i_enterprise_nm = 'SCP_STARTS' THEN
      OPEN cur_cal;
      LOOP
        fetch cur_cal bulk collect
          into curval_cal limit 1000;
      
        forall i in 1 .. curval_cal.count
          INSERT INTO calendardetail_stg
            (calendarname,
             siteid,
             item_name,
             effenddate,
             effstartdate,
             engine_id,
             enterprise,
             patternname,
             patternseq,
             priority,
             entry_id,
             yield,
             sourcedate,
             calendartype,
             shiftnumber)
          VALUES
            (curval_cal   (i).calendarname,
             NULL,
             NULL,
             curval_cal   (i).effenddate,
             curval_cal   (i).effstartdate,
             curval_cal   (i).engine_id,
             curval_cal   (i).enterprise,
             'DAYS_OF_WEEK',
             curval_cal   (i).patternseq,
             '1',
             NULL,
             NULL,
             curval_cal   (i).sourcedate,
             NULL,
             '1');
      
        forall i in 1 .. curval_cal.count
          INSERT INTO calbasedattr_stg
            (calendarname,
             engine_id,
             enterprise,
             patternseq,
             attribute,
             value,
             sourcedate,
             shiftnumber,
             valueuom)
          VALUES
            (curval_cal         (i).calendarname,
             curval_cal         (i).engine_id,
             curval_cal         (i).enterprise,
             curval_cal         (i).patternseq,
             'AVAILABLE_CAPACITY',
             curval_cal         (i).eff_res_cnt,
             curval_cal         (i).sourcedate,
             '1',
             'UNITS');
      
        forall i in 1 .. curval_cal.count
          INSERT INTO calpatdetail_stg
            (attribute,
             attributevalue,
             calendarname,
             engine_id,
             enterprise,
             patternseq,
             sourcedate)
          VALUES
            ('DAYS',
             curval_cal(i).attr_value,
             curval_cal(i).calendarname,
             curval_cal(i).engine_id,
             curval_cal(i).enterprise,
             curval_cal(i).patternseq,
             curval_cal(i).sourcedate);
      
        EXIT WHEN cur_cal%NOTFOUND;
      END LOOP;
    
    ELSE
    
      -- This cursor is used to load data for Sort and Test resources
      DBMS_OUTPUT.PUT_LINE(1 || '-' || SYSTIMESTAMP || i_enterprise_nm);
      OPEN cur_caldetail;
    
      LOOP
        FETCH cur_caldetail BULK COLLECT
          INTO curval_caldetail LIMIT 1000;
      
        FORALL i IN 1 .. curval_caldetail.count --Save exceptions
          INSERT INTO SCMDATA.Calendardetail_stg --Inserting into Calendardetail
            (CALENDARNAME,
             SITEID,
             ITEM_NAME,
             EFFENDDATE,
             EFFSTARTDATE,
             ENGINE_ID,
             ENTERPRISE,
             PATTERNNAME,
             PATTERNSEQ,
             PRIORITY,
             ENTRY_ID,
             YIELD,
             sourcedate,
             CALENDARTYPE,
             SHIFTNUMBER)
          VALUES
            (curval_caldetail(i).calendarname,
             curval_caldetail(i).siteid,
             NULL,
             curval_caldetail(i).effenddate,
             curval_caldetail(i).effstartdate,
             curval_caldetail(i).engine_id,
             i_enterprise_nm,
             'DAYS_OF_WEEK',
             curval_caldetail(i).patternseq,
             '1',
             NULL,
             NULL,
             sysdate,
             NULL,
             1);
      
        FORALL i IN 1 .. curval_caldetail.count --Save exceptions
          INSERT INTO SCMDATA.CALBASEDATTR_stg --Inserting into CALBASEDATTR
            (CALENDARNAME,
             ENGINE_ID,
             ENTERPRISE,
             PATTERNSEQ,
             ATTRIBUTE,
             VALUE,
             sourcedate,
             SHIFTNUMBER,
             VALUEUOM)
          VALUES
            (curval_caldetail   (i).calendarname,
             curval_caldetail   (i).engine_id,
             i_enterprise_nm,
             curval_caldetail   (i).patternseq,
             'AVAILABLE_CAPACITY',
             curval_caldetail   (i).value,
             sysdate,
             1, -- SHIFTNUMBER hardcode as 1 added 8/3/2017
             curval_caldetail   (i).valueuom); -- VALUEUOM hardcode to 'hours' added 9/1/2017
      
        FORALL i IN 1 .. curval_caldetail.count --Save exceptions
        --Inserting into CALPATDETAIL
          INSERT INTO SCMDATA.CALPATDETAIL_stg
            (ATTRIBUTE,
             ATTRIBUTEVALUE,
             CALENDARNAME,
             ENGINE_ID,
             ENTERPRISE,
             PATTERNSEQ,
             sourcedate)
          VALUES
            (curval_caldetail(i).attribute, --'days',
             curval_caldetail(i).attr_value,
             curval_caldetail(i).calendarname,
             curval_caldetail(i).engine_id,
             i_enterprise_nm,
             curval_caldetail(i).patternseq,
             sysdate);
      
        EXIT WHEN cur_caldetail%NOTFOUND;
      
      END LOOP;
    
      --------code for NON ATe----
      DBMS_OUTPUT.PUT_LINE(2 || '-' || SYSTIMESTAMP || i_enterprise_nm);
      OPEN cur_caldetail_nonate;
    
      LOOP
        FETCH cur_caldetail_nonate BULK COLLECT
          INTO curval_caldetail_nonate LIMIT 1000;
      
        FORALL i IN 1 .. curval_caldetail_nonate.count --Save exceptions
          INSERT INTO SCMDATA.Calendardetail_stg --Inserting into Calendardetail
            (CALENDARNAME,
             SITEID,
             ITEM_NAME,
             EFFENDDATE,
             EFFSTARTDATE,
             ENGINE_ID,
             ENTERPRISE,
             PATTERNNAME,
             PATTERNSEQ,
             PRIORITY,
             ENTRY_ID,
             YIELD,
             sourcedate,
             CALENDARTYPE,
             SHIFTNUMBER)
          VALUES
            (curval_caldetail_nonate(i).calendarname,
             curval_caldetail_nonate(i).siteid,
             NULL,
             curval_caldetail_nonate(i).effenddate,
             curval_caldetail_nonate(i).effstartdate,
             curval_caldetail_nonate(i).engine_id,
             i_enterprise_nm,
             'DAYS_OF_WEEK',
             curval_caldetail_nonate(i).patternseq,
             '1',
             NULL,
             NULL,
             sysdate,
             NULL,
             1);
      
        FORALL i IN 1 .. curval_caldetail_nonate.count --Save exceptions
          INSERT INTO SCMDATA.CALBASEDATTR_stg --Inserting into CALBASEDATTR
            (CALENDARNAME,
             ENGINE_ID,
             ENTERPRISE,
             PATTERNSEQ,
             ATTRIBUTE,
             VALUE,
             sourcedate,
             SHIFTNUMBER,
             VALUEUOM)
          VALUES
            (curval_caldetail_nonate(i).calendarname,
             curval_caldetail_nonate(i).engine_id,
             i_enterprise_nm,
             curval_caldetail_nonate(i).patternseq,
             'AVAILABLE_CAPACITY',
             curval_caldetail_nonate(i).value,
             sysdate,
             1, -- SHIFTNUMBER hardcode as 1 added 8/3/2017
             curval_caldetail_nonate(i).valueuom); -- VALUEUOM hardcode to 'hours' added 9/1/2017
      
        FORALL i IN 1 .. curval_caldetail_nonate.count --Save exceptions
        --Inserting into CALPATDETAIL
          INSERT INTO SCMDATA.CALPATDETAIL_stg
            (ATTRIBUTE,
             ATTRIBUTEVALUE,
             CALENDARNAME,
             ENGINE_ID,
             ENTERPRISE,
             PATTERNSEQ,
             sourcedate)
          VALUES
            (curval_caldetail_nonate(i).attribute, --'days',
             curval_caldetail_nonate(i).attr_value,
             curval_caldetail_nonate(i).calendarname,
             curval_caldetail_nonate(i).engine_id,
             i_enterprise_nm,
             curval_caldetail_nonate(i).patternseq,
             sysdate);
      
        EXIT WHEN cur_caldetail_nonate%NOTFOUND;
      
      END LOOP;
    
      ---------------------------------------------------------------------------------------------------------------------------------------------------------
    
      --This code  to load the Assembly/Bump/Diecoat resources
    
      -- NULL;
    
      --Cursor to get all the site and resources with capacities
    
      /*  delete from CALBASEDATTR_stg
       where enterprise = 'SCP_DAILY'
         and attribute = 'AVAILABLE_CAPACITY';
      
      delete from CALENDARDETAIL_STG
       where enterprise = 'SCP_DAILY'
         and patternname = 'EVERYDAY';*/
    
      -- commit;
      -- Get the bucketsizr that decides the number of daily rows
      select bucketsize
        into ln_asmbly_days
        from odsscp.Bucketpattern
       where BUCKETSIZEUOM = 'day'
         and BUCKETNAME = 'LP_HORIZON'
         and enterprise = i_enterprise_nm;
    
      ld_rpt_startdt := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
      ld_rpt_enddt   := scmdata.engines.get_plan_effenddate(i_enterprise_nm);
    
      -- dbms_output.put_line(ld_rpt_startdt);
      DBMS_OUTPUT.PUT_LINE(3 || '-' || SYSTIMESTAMP || i_enterprise_nm);
      FOR curval_asmbly_dly IN cur_caldetail_asmbly_dly LOOP
      
        INSERT INTO SCMDATA.Calendardetail_stg --Inserting into Calendardetail
          (CALENDARNAME,
           SITEID,
           ITEM_NAME,
           EFFENDDATE,
           EFFSTARTDATE,
           ENGINE_ID,
           ENTERPRISE,
           PATTERNNAME,
           PATTERNSEQ,
           PRIORITY,
           ENTRY_ID,
           YIELD,
           sourcedate,
           CALENDARTYPE,
           SHIFTNUMBER)
        VALUES
          (curval_asmbly_dly.calendarname,
           curval_asmbly_dly.siteid,
           NULL,
           curval_asmbly_dly.effenddate,
           curval_asmbly_dly.effstartdate,
           curval_asmbly_dly.engine_id,
           i_enterprise_nm,
           'EVERYDAY',
           curval_asmbly_dly.patternseq,
           '1',
           NULL,
           NULL,
           sysdate,
           NULL,
           1);
      
        INSERT INTO SCMDATA.CALBASEDATTR_stg --Inserting into CALBASEDATTR
          (CALENDARNAME,
           ENGINE_ID,
           ENTERPRISE,
           PATTERNSEQ,
           ATTRIBUTE,
           VALUE,
           sourcedate,
           SHIFTNUMBER,
           VALUEUOM)
        VALUES
          (curval_asmbly_dly.calendarname,
           curval_asmbly_dly.engine_id,
           i_enterprise_nm,
           curval_asmbly_dly.patternseq,
           'AVAILABLE_CAPACITY',
           curval_asmbly_dly.value,
           sysdate,
           1, -- SHIFTNUMBER hardcode as 1 added 8/3/2017
           curval_asmbly_dly.valueuom); -- VALUEUOM hardcode to 'hours' added 9/1/2017
      
      END LOOP;
    
      --    dbms_output.put_line('done');
      DBMS_OUTPUT.PUT_LINE(3.1 || '-' || SYSTIMESTAMP);
      OPEN cur_caldetail_asmbly;
    
      LOOP
        FETCH cur_caldetail_asmbly BULK COLLECT
          INTO curval_caldetail_asmbly LIMIT 10000;
      
        FORALL i IN 1 .. curval_caldetail_asmbly.count --Save exceptions
          INSERT INTO SCMDATA.Calendardetail_stg --Inserting into Calendardetail
            (CALENDARNAME,
             SITEID,
             ITEM_NAME,
             EFFENDDATE,
             EFFSTARTDATE,
             ENGINE_ID,
             ENTERPRISE,
             PATTERNNAME,
             PATTERNSEQ,
             PRIORITY,
             ENTRY_ID,
             YIELD,
             sourcedate,
             CALENDARTYPE,
             SHIFTNUMBER)
          VALUES
            (curval_caldetail_asmbly(i).calendarname,
             curval_caldetail_asmbly(i).siteid,
             NULL,
             curval_caldetail_asmbly(i).effenddate,
             curval_caldetail_asmbly(i).effstartdate,
             curval_caldetail_asmbly(i).engine_id,
             i_enterprise_nm,
             'EVERYDAY',
             curval_caldetail_asmbly(i).patternseq,
             '1',
             NULL,
             NULL,
             sysdate,
             NULL,
             1);
      
        FORALL i IN 1 .. curval_caldetail_asmbly.count --Save exceptions
          INSERT INTO SCMDATA.CALBASEDATTR_stg --Inserting into CALBASEDATTR
            (CALENDARNAME,
             ENGINE_ID,
             ENTERPRISE,
             PATTERNSEQ,
             ATTRIBUTE,
             VALUE,
             sourcedate,
             SHIFTNUMBER,
             VALUEUOM)
          VALUES
            (curval_caldetail_asmbly(i).calendarname,
             curval_caldetail_asmbly(i).engine_id,
             i_enterprise_nm,
             curval_caldetail_asmbly(i).patternseq,
             'AVAILABLE_CAPACITY',
             curval_caldetail_asmbly(i).value,
             sysdate,
             1, -- SHIFTNUMBER hardcode as 1 added 8/3/2017
             curval_caldetail_asmbly(i).valueuom); -- VALUEUOM hardcode to 'hours' added 9/1/2017
      
        EXIT WHEN cur_caldetail_asmbly%NOTFOUND;
      
      END LOOP;
    
      Commit; /* remove later  */
    
    END IF;
    frmdata.logs.info('CALENDARDETAIL load from SRC to ODS is completed');
    frmdata.logs.info('CALBASEDATTR load from SRC to ODS is completed');
    frmdata.logs.info('CALPATDETAIL load from SRC to ODS is completed');
  
  END load_CALENDARDETAIL;

  --************************************************************
  --PROCEDURE to load data into ROUTE_RES_ALT_SRC -- Header table
  --  with  PROD_RTE_SEQ_ID,
  --     PRIM_RES_SET_ID as keys
  -- This procedure is used to load data for Sort and Test resources
  --************************************************************

  PROCEDURE load_RES_RTE_ASN_SRC(i_enterprise_nm IN VARCHAR2) IS
  
    -- lv_sysdate DATE := SCMDATA.Dates.get_rpt_dt; --to get sysdate date
  
  BEGIN
  
    frmdata.logs.begin_log;
    --  frmdata.logs.info('Params:' || i_enterprise_nm);
    frmdata.delete_data('RES_RTE_ASN_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    --  lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date'23-May-2017';
    lv_rpt_dt := SCMDATA.Dates.get_rpt_dt;
  
    lv_row_source := 'P001';
  
    --************************************************************
    -- Join ROUTING_RES_ALT_SRC.SAP_ROUTE_ID, ROUTING_RES_ALT_SRC.eng_rte_id (without underscore),
    -- ROUTING_RES_ALT_SRC.eff_start_date and ROUTING_RES_ALT_SRC.eff_end_date
    -- with  res_part_asn_src.SAP_ROUTE_ID, PART_RES_ASN_SRC.eng_rte_id,PART_RES_ASN_SRC.eff_start_dt and eff_end_Date
    -- and get prod_route_seq_id and Prim_resource_set_id
    --************************************************************
    ----Loads SORT and TEST resources--------------
    -- logs.debug('INSERT');
    INSERT INTO scmdata.RES_RTE_ASN_SRC
      (ENTERPRISE,
       PROD_RTE_SEQ_ID,
       PRIM_RES_SET_ID,
       MFG_PART_NUM,
       MATL_TYPE_CD,
       SAP_RTE_ID,
       ENG_SITE_NUM,
       ENG_RTE_ID,
       RTE_SEQ_NUM,
       ENG_OPER_SEQ,
       RES_AREA,
       EFF_START_DT,
       EFF_END_DT,
       SOURCE_DT, --Source dt -- sysdate
       ETL_DT, --ETL date
       ROW_SOURCE)
      SELECT i_enterprise_nm,
             pra.PROD_RTE_SEQ_ID,
             pra. PRIM_RES_SET_ID,
             rhs.mfg_part_num,
             rhs.matl_type_cd,
             rhs.sap_rte_id,
             rhs.eng_site_num,
             rhs.eng_rte_id,
             --pra.eng_routing as part_engine_routingid,
             pra.RTE_SEQ_NUM,
             pra.ENG_OPER_SEQ,
             pra.res_area,
             rhs.eff_start_dt,
             rhs.eff_end_dt,
             sysdate,
             rhs.source_dt,
             'RRA-01'
      /*
      pra.eff_start_dt,
      pra.eff_end_dt*/
        FROM scmdata.rte_header_src   rhs, --.routing_res_alt_src rhs,
             scmdata.RES_PART_ASN_SRC pra
       WHERE rhs.sap_rte_id = pra.sap_rte_id
         AND pra.enterprise = i_enterprise_nm --added by SA on 1/10
         and pra.res_area <> 'NON ATE FT'
            /*and rhs.eff_start_dt = pra.eff_start_dt
            and rhs.eff_end_dt = pra.eff_end_dt*/
         and (greatest(trunc((NVL(rhs.eff_start_dt, lv_rpt_dt))), lv_rpt_dt) BETWEEN
             trunc(NVL(pra.eff_start_dt, lv_rpt_dt)) and
             trunc(NVL(pra.eff_end_dt, '1-jan-2099'))) --
         and trunc(NVL(rhs.eff_end_dt, '1-jan-2099')) <=
             trunc(NVL(pra.eff_end_dt, '1-jan-2099')) --Added by Sravani on 7/10 when bug fixing with Sraveswar.
         and rhs.mfg_part_num = pra.mfg_part_num
         and rhs.eng_rte_id like '%' || pra.eng_rte_id || '%'; ----Added by VS 01/11/18
    /*and pra.ENG_RTE_ID = --in routing_header_src for eng_routing column removing value after site id and comparing
    (SELECT substr(rhs2.eng_rte_id,
                   1,
                   (SELECT instr(rhs1.eng_rte_id, smr1.eng_site_num) --to get position of the eng_site_num
                      FROM scmdata.rte_header_src rhs1, --g_res_alt_src rhs1,
                           (select distinct eng_site_num
                              from ref_data.site_master_ref) smr1
                     WHERE rhs1.eng_rte_id like
                           '%' || smr1.eng_site_num || '%' -- rhs1.eng_facility=smr1.eng_site_num
                       AND rhs1.eng_rte_id = rhs.eng_rte_id) +
                   length(smr2.eng_site_num) - 1)
       FROM scmdata.rte_header_src rhs2,
            (select distinct eng_site_num
               from ref_data.site_master_ref) smr2
      WHERE rhs2.eng_rte_id like '%' || smr2.eng_site_num || '%' -- rhs2.eng_facility=smr2.eng_site_num
        AND rhs2.eng_rte_id = rhs.eng_rte_id)*/
  
    -----------Loads NON ATE resources-----
    insert into RES_RTE_ASN_SRC
      (ENTERPRISE,
       MFG_PART_NUM,
       MATL_TYPE_CD,
       SAP_RTE_ID,
       ENG_SITE_NUM,
       ENG_RTE_ID,
       RTE_SEQ_NUM,
       ENG_OPER_SEQ,
       EFF_START_DT,
       EFF_END_DT,
       PROD_RTE_SEQ_ID,
       PRIM_RES_SET_ID,
       RES_AREA, ------Added res_area column to distinguish if a res_nm is ATE or NON ATE by VS 11/10/2017
       SOURCE_DT,
       ETL_DT,
       ROW_SOURCE)
      select distinct eer.enterprise,
                      rhs.mfg_part_num,
                      rhs.matl_type_cd,
                      rhs.sap_rte_id,
                      rhs.eng_site_num,
                      rhs.eng_rte_id,
                      rpa.rte_seq_num,
                      rpa.eng_oper_seq,
                      rhs.eff_start_dt,
                      rhs.eff_end_dt,
                      rpa.prod_rte_seq_id,
                      rpa.prim_res_set_id,
                      rpa.res_area,
                      sysdate,
                      rhs.source_dt,
                      'RRA-02'
        from ref_data.eng_enterprise_ref eer,
             rte_header_src              rhs,
             res_part_asn_src            rpa
       where eer.enterprise = i_enterprise_nm
         AND eer.enterprise = rpa.enterprise --added by SA on 1/10
         and rpa.res_area = 'NON ATE FT'
         AND eer.ent_res_flg like '%:' || rpa.RES_AREA || ':%' --added by SA on 12/22
         and rhs.sap_rte_id = rpa.sap_rte_id
         and rhs.mfg_part_num = rpa.mfg_part_num
         and rhs.eng_rte_id like '%' || rpa.eng_rte_id || '%'
         and (greatest(trunc((NVL(rhs.eff_start_dt, l_curr_dt))), l_curr_dt) BETWEEN
             trunc(NVL(rpa.eff_start_dt, l_curr_dt)) and
             trunc(NVL(rpa.eff_end_dt, '1-jan-2099')))
         and trunc(NVL(rhs.eff_end_dt, '1-jan-2099')) <=
             trunc(NVL(rpa.eff_end_dt, '1-jan-2099'));
  
    Commit; /* remove later */
  
    frmdata.logs.info('RES_RTE_ASN_SRC load from SNP to SRC is completed');
  END load_RES_RTE_ASN_SRC;

  --************************************************************
  --PROCEDURE to update route and resource flags in ROUTE_RES_ALT_SRC -- Header table
  --  -- This procedure is used to load data for Sort and Test resources
  --************************************************************

  PROCEDURE UPD_RES_RTE_ASN_SRC_FLG(i_enterprise_nm IN VARCHAR2) IS
    ----Variable declaration FOR ATE---------------
    ln_cnt_tester    INTEGER := 0;
    ln_cnt_handler   INTEGER := 0;
    ln_cnt_hw_set    INTEGER := 0;
    lv_alt_route_flg VARCHAR2(1) := 'N';
    lv_alt_rsrc_flg  VARCHAR2(1) := 'N';
    -- lv_rsrc_type            VARCHAR2(64);
    --  lv_hardware_type        VARCHAR2(64);
    -- lv_alt_res_id           VARCHAR2(256) := NULL;
    --  lv_alt_rte_res_id       VARCHAR2(128) := NULL;
    --  lv_alt_res_id_cur       VARCHAR2(256) := NULL;
    lv_prod_rte_seq_id RES_PART_ASN_SRC. PROD_RTE_SEQ_ID%TYPE;
    lv_prim_res_set_id RES_PART_ASN_SRC.PRIM_RES_SET_ID%TYPE;
  
    ---Variable declaration for NON ATE-----
  
    -----below variables are to update alt_res, alt_rte flgs in res_rte_asn_src-------------
    ln_cnt_bibr           INTEGER := 0;
    ln_cnt_biov           INTEGER := 0;
    lv_alt_route_flg_na   VARCHAR2(1) := 'N';
    lv_alt_rsrc_flg_na    VARCHAR2(1) := 'N';
    lv_prod_rte_seq_id_na RES_PART_ASN_SRC. PROD_RTE_SEQ_ID%TYPE;
    lv_prim_res_set_id_na RES_PART_ASN_SRC.PRIM_RES_SET_ID%TYPE;
  
    -- We have the clone table scmdata.RES_RTE_ASN_SRC table populated now we need to run the logic to update
    -- alt_rte_fg , alt_res_fg
    -----below two cursors for ate----------
    CURSOR cur_res_alt_src IS
      select ENTERPRISE,
             ENG_RTE_ID,
             PROD_RTE_SEQ_ID,
             PRIM_RES_SET_ID,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             ETL_DT
        from scmdata.RES_RTE_ASN_SRC
       where res_area <> 'NON ATE FT'
         and enterprise = i_enterprise_nm; ---Added by VS 01/11/18
    --WHERE  prod_route_seq_id =1384 and prim_resource_set_id = 2236;
  
    -- CURSOR to get resource_set_ids for a given prim_resource_set_id from the res_attr_src table
  
    CURSOR cur_res_attr_src IS
      select distinct res_set_id resource_set_id
        from res_attr_src
       where prod_rte_seq_id = lv_prod_rte_seq_id
         and prim_res_set_id = lv_prim_res_set_id
         and prim_res_set_id != res_set_id;
  
    curval_res_alt  cur_res_alt_src%ROWTYPE;
    curval_res_attr cur_res_attr_src%ROWTYPE;
  
    -------below cursors for non ate--------------
  
    ---cursor to update alt_rte,alt_res flg----
    CURSOR cur_res_alt_nonate IS
      select ENTERPRISE,
             ENG_RTE_ID,
             PROD_RTE_SEQ_ID,
             PRIM_RES_SET_ID,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             ETL_DT
        from scmdata.RES_RTE_ASN_SRC
       where res_area = 'NON ATE FT'
         and enterprise = i_enterprise_nm; ---Added by VS 01/11/18
    ---cursor to get prim, alt resources
    CURSOR cur_res_nonattr_src IS
      select distinct res_set_id
        from res_nonate_attr_src
       where prod_rte_seq_id = lv_prod_rte_seq_id_na
         and prim_res_set_id = lv_prim_res_set_id_na
         and prim_res_set_id <> res_set_id
         and enterprise = i_enterprise_nm; ---Added by VS 01/11/18;
  
    curval_res_alt_na  cur_res_alt_nonate%ROWTYPE;
    curval_res_attr_na cur_res_nonattr_src%ROWTYPE;
  
  BEGIN
  
    ----below logic updates ATE alt_res, alt_rte_flg---------------
    --  lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date'23-May-2017';
  
    --lv_row_source := 'U001';
  
    FOR curval_res_alt IN cur_res_alt_src LOOP
    
      ln_cnt_tester    := 0;
      ln_cnt_handler   := 0;
      ln_cnt_hw_set    := 0;
      lv_alt_route_flg := 'N';
      lv_alt_rsrc_flg  := 'N';
      --  lv_alt_res_id     := NULL;
      --  lv_alt_rte_res_id := NULL;
    
      lv_prod_rte_seq_id := curval_res_alt.prod_rte_seq_id;
      lv_prim_res_set_id := curval_res_alt.prim_res_set_id;
    
      --  lv_row_source := lv_row_source ||'|A001';
    
      FOR curval_res_attr IN cur_res_attr_src LOOP
      
        -- For a given resource_set_id find if any Tester/Handler/Hardware set id is different than the primary
      
        select count(res_nm)
          INTO ln_cnt_tester
          from res_attr_src res
         where --prod_route_seq_id = 1027 and prim_resource_set_id = 2036
         prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
         and prim_res_set_id = curval_res_alt.prim_res_set_id
         and res_type = 'TESTER'
        -- and   prim_resource_set_id != resource_set_id
         and res_set_id = curval_res_attr.resource_set_id
         and nvl(res_nm, 'dflt') !=
         (select nvl(res_nm, 'dflt')
            from res_attr_src pri
           where pri.prod_rte_seq_id = res.prod_rte_seq_id
             and pri.prim_res_set_id = res.prim_res_set_id
                --where pri.prod_route_seq_id = 1027 and pri.prim_resource_set_id = 2036
             and pri.res_type = 'TESTER'
             and pri.res_set_id = res.prim_res_set_id);
      
        select count(res_nm)
          INTO ln_cnt_handler
          from res_attr_src res
         where prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
           and prim_res_set_id = curval_res_alt.prim_res_set_id
           and res_type = 'HANDLER'
              -- and   prim_resource_set_id != resource_set_id
           and res_set_id = curval_res_attr.resource_set_id
           and nvl(res_nm, 'dflt') !=
               (select nvl(res_nm, 'dflt')
                  from res_attr_src pri
                 where pri.prod_rte_seq_id = res.prod_rte_seq_id
                   and pri.prim_res_set_id = res.prim_res_set_id
                   and pri.res_type = 'HANDLER'
                   and pri.res_set_id = res.prim_res_set_id);
      
        select count(*)
          INTO ln_cnt_hw_set
          from (select distinct hw_type ALT_RES_Resource_TYPE,
                                hw_nm   ALT_RES_Resource_nm
                  from res_attr_src sec
                 where prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
                   and prim_res_set_id = curval_res_alt.prim_res_set_id
                   and res_type = 'HARDWARE_SET'
                      -- and   prim_resource_set_id != resource_set_id
                   and res_set_id = curval_res_attr.resource_set_id
                MINUS
                select hw_type ALT_RES_Resource_TYPE,
                       hw_nm   ALT_RES_Resource_nm
                  from res_attr_src pri
                 where prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
                   and prim_res_set_id = curval_res_alt.prim_res_set_id
                   and res_type = 'HARDWARE_SET'
                   and prim_res_set_id = res_set_id);
      
        -- If any two resource_types are different set alt route as 'Y' no need to check other resource_set_ids can just quit
        -- New condition addded 7/19/2017 bbhatia when 2 or more HARDWARE_SETs are different set alt_route_flg as 'Y'
      
        IF (ln_cnt_tester > 0 AND ln_cnt_handler > 0) THEN
          lv_alt_route_flg := 'Y';
        ELSIF (ln_cnt_handler > 0 AND ln_cnt_hw_set > 0) THEN
          lv_alt_route_flg := 'Y';
        ELSIF (ln_cnt_tester > 0 AND ln_cnt_hw_set > 0) THEN
          lv_alt_route_flg := 'Y';
        ELSIF ln_cnt_hw_set > 1 THEN
          lv_alt_route_flg := 'Y';
        
          -- If only one resource is  different set alt resource as 'Y'
          -- -----------------------------------------------------------------------------
        
        ELSIF (ln_cnt_tester > 0 AND ln_cnt_handler = 0 AND
              ln_cnt_hw_set = 0) THEN
          lv_alt_rsrc_flg := 'Y';
        
        ELSIF (ln_cnt_handler > 0 AND ln_cnt_tester = 0 AND
              ln_cnt_hw_set = 0) THEN
          lv_alt_rsrc_flg := 'Y';
        
        ELSIF (ln_cnt_hw_set = 1 AND ln_cnt_handler = 0 AND
              ln_cnt_tester = 0) THEN
          lv_alt_rsrc_flg := 'Y';
        
        END IF;
      
        IF lv_alt_route_flg = 'Y' THEN
        
          lv_alt_rsrc_flg := 'N';
          EXIT;
          -- exit on checking any further resource_set_id's for the primary exit loop for cur_res_attr_src
        
        END IF;
      
      END LOOP;
    
      UPDATE RES_RTE_ASN_SRC
         SET alt_rte_flg = lv_alt_route_flg,
             alt_res_flg = lv_alt_rsrc_flg,
             source_dt   = sysdate, -- lv_cur_date,
             -- alt_res_id      = lv_alt_res_id,
             --alt_rte_res_id  = lv_alt_rte_res_id,
             row_source = 'UPD-01'
       WHERE --prod_route_seq_id = curval_res_alt.prod_route_seq_id
      --AND prim_resource_set_id = curval_res_alt.prim_resource_set_id    AND
       enterprise = curval_res_alt.enterprise
       AND eng_rte_id = curval_res_alt.eng_rte_id
       and res_area <> 'NON ATE FT'
       AND prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
       AND prim_res_set_id = curval_res_alt.prim_res_set_id
       AND rte_seq_num = curval_res_alt.rte_seq_num
       AND eng_oper_seq = curval_res_alt.eng_oper_seq;
    
      commit;
    
    END LOOP;
  
    ----------below logic updates non ate alt_res, alt_rte flgs----------
  
    for curval_res_alt_na in cur_res_alt_nonate loop
      ln_cnt_bibr         := 0;
      ln_cnt_biov         := 0;
      lv_alt_route_flg_na := 'N';
      lv_alt_rsrc_flg_na  := 'N';
    
      lv_prod_rte_seq_id_na := curval_res_alt_na.prod_rte_seq_id;
      lv_prim_res_set_id_na := curval_res_alt_na.prim_res_set_id;
    
      for curval_res_attr_na in cur_res_nonattr_src loop
      
        select count(res_nm)
          into ln_cnt_bibr
          from res_nonate_attr_src rna
         where prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
           and prim_res_set_id = curval_res_alt_na.prim_res_set_id
           and res_set_id = curval_res_attr_na.res_set_id
           and res_type = 'BIBR'
           and enterprise = i_enterprise_nm ---Added by VS 01/11/18
           and nvl(res_nm, 'dflt') <>
               (select nvl(res_nm, 'dflt')
                  from res_nonate_attr_src resn
                 where resn.prod_rte_seq_id = rna.prod_rte_seq_id
                   and resn.prim_res_set_id = rna.prim_res_set_id
                   and resn.res_type = 'BIBR'
                   and resn.res_set_id = rna.prim_res_set_id
                   and enterprise = i_enterprise_nm); ---Added by VS 01/11/18
      
        select count(res_nm)
          into ln_cnt_biov
          from res_nonate_attr_src rna
         where prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
           and prim_res_set_id = curval_res_alt_na.prim_res_set_id
           and res_set_id = curval_res_attr_na.res_set_id
           and res_type = 'BIOV'
           and enterprise = i_enterprise_nm ---Added by VS 01/11/18
           and nvl(res_nm, 'dflt') <>
               (select nvl(res_nm, 'dflt')
                  from res_nonate_attr_src resn
                 where resn.prod_rte_seq_id = rna.prod_rte_seq_id
                   and resn.prim_res_set_id = rna.prim_res_set_id
                   and resn.res_type = 'BIOV'
                   and enterprise = i_enterprise_nm ---Added by VS 01/11/18
                   and resn.res_set_id = rna.prim_res_set_id);
      
        if (ln_cnt_bibr > 0 and ln_cnt_biov > 0) then
          lv_alt_route_flg_na := 'Y';
        elsif (ln_cnt_bibr > 0 and ln_cnt_biov = 0) then
          lv_alt_rsrc_flg_na := 'Y';
        elsif (ln_cnt_bibr = 0 and ln_cnt_biov > 0) then
          lv_alt_rsrc_flg_na := 'Y';
        end if;
      
        if lv_alt_route_flg_na = 'Y' then
          lv_alt_rsrc_flg_na := 'N';
          exit;
        end if;
      end loop;
    
      update res_rte_asn_src
         set alt_rte_flg = lv_alt_route_flg_na,
             alt_res_flg = lv_alt_rsrc_flg_na,
             source_dt   = sysdate,
             row_source  = 'UPD-02'
       where enterprise = curval_res_alt_na.enterprise
         AND eng_rte_id = curval_res_alt_na.eng_rte_id
         AND prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
         AND prim_res_set_id = curval_res_alt_na.prim_res_set_id
         AND rte_seq_num = curval_res_alt_na.rte_seq_num
         AND eng_oper_seq = curval_res_alt_na.eng_oper_seq
         and res_area = 'NON ATE FT';
      commit;
    end loop;
  
  END UPD_RES_RTE_ASN_SRC_FLG;

  --************************************************************
  --PROCEDURE to load data into  ROUTE_RES_ALT_DTL_SRC detail table for records
  --where alt_res_flg = 'N' and alt_rte_flg = 'N'
  -- This procedure is used to load data for Sort and Test resources
  --************************************************************

  PROCEDURE LOAD_RES_RTE_ASN_DTLNN_SRC(i_enterprise_nm IN VARCHAR2) IS
  
  BEGIN
  
    frmdata.logs.begin_log;
    frmdata.delete_data('RES_RTE_ASN_DTL_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    --  lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date'23-May-2017';
    --  lv_rpt_dt := SCMDATA.Dates.get_rpt_dt;
  
    lv_row_source := 'X001';
    INSERT INTO RES_RTE_ASN_DTL_SRC
      (ENTERPRISE,
       ENG_RTE_ID,
       PROD_RTE_SEQ_ID,
       PRIM_RES_SET_ID,
       RTE_SEQ_NUM,
       ENG_OPER_SEQ,
       RES_AREA,
       ALT_RTE_FLG,
       ALT_RES_FLG,
       ALT_RES_TYPE,
       ALT_RES_SET_ID,
       ALT_RES_NM,
       ALT_RES_PRIM_RES_NM,
       ALT_RES_HW_SET_ID,
       ALT_RES_UPH,
       SOURCE_DT,
       ETL_DT,
       ROW_SOURCE)
      SELECT head.ENTERPRISE, -- 1050 rows
             head.ENG_RTE_ID,
             head.PROD_RTE_SEQ_ID,
             head.PRIM_RES_SET_ID,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             head.res_area,
             ALT_RTE_FLG,
             ALT_RES_FLG,
             CASE
               WHEN prim.res_type = 'HARDWARE_SET' THEN
                prim.res_type || '_' || prim.hw_type
               ELSE
                prim.res_type
             END ALT_RES_RESOURCE_TYPE,
             null, --  ALT_RESOURCE_SET_ID,
             null, -- ALT_RES_RESOURCE_NM
             CASE
               WHEN prim.res_type = 'HARDWARE_SET' THEN
                prim.hw_nm
               ELSE
                prim.res_nm
             END AS ALT_RESOURCE_NAME,
             null, --  ALT_RES_HW_SET_ID,
             prim.uph,
             sysdate,
             head.etl_dt,
             lv_row_source || '-' || '2'
        from RES_RTE_ASN_SRC head, res_attr_src prim
       where head.alt_rte_flg = 'N'
         and head.alt_res_flg = 'N'
         AND head.enterprise = i_enterprise_nm --added by SA on 1/10
         and head.res_area <> 'NON ATE FT'
         and prim.prod_rte_seq_id = head.prod_rte_seq_id
         and prim.prim_res_set_id = head.prim_res_set_id
         and prim.res_type in ('TESTER', 'HANDLER', 'HARDWARE_SET')
         and prim.res_nm is not null;
  
    -----------------Load NON ATE resources-------
    insert into res_rte_asn_dtl_src
      (ENTERPRISE,
       ENG_RTE_ID,
       PROD_RTE_SEQ_ID,
       PRIM_RES_SET_ID,
       RTE_SEQ_NUM,
       ENG_OPER_SEQ,
       res_area,
       ALT_RTE_FLG,
       ALT_RES_FLG,
       ALT_RES_TYPE,
       ALT_RES_SET_ID,
       ALT_RES_NM,
       ALT_RES_PRIM_RES_NM,
       ALT_RES_HW_SET_ID,
       ALT_RES_UPH,
       SOURCE_DT,
       ETL_DT,
       ROW_SOURCE)
      select distinct rrte.enterprise,
                      rrte.eng_rte_id,
                      rrte.prod_rte_seq_id,
                      rrte.prim_res_set_id,
                      rrte.rte_seq_num,
                      rrte.eng_oper_seq,
                      rrte.res_area,
                      rrte.alt_rte_flg,
                      rrte.alt_res_flg,
                      nattr.res_type alt_res_type,
                      null ALT_RESOURCE_SET_ID,
                      null ALT_RES_RESOURCE_NM,
                      res_nm alt_res_nm,
                      null ALT_RES_HW_SET_ID,
                      null uph,
                      sysdate,
                      rrte.etl_dt,
                      lv_row_source || '-' || '19'
        from res_rte_asn_src rrte, res_nonate_attr_src nattr
       where rrte.alt_rte_flg = 'N'
         and rrte.alt_res_flg = 'N'
         and rrte.prod_rte_seq_id = nattr.prod_rte_seq_id
         and rrte.prim_res_set_id = nattr.prim_res_set_id
         AND rrte.enterprise = i_enterprise_nm --added by SA on 1/10
         AND rrte.enterprise = nattr.enterprise --added by SA on 1/10
         and nattr.res_type in ('BIBR', 'BIOV')
         and nattr.res_nm is not null
         and rrte.res_area = 'NON ATE FT';
  
    Commit; /* remove later */
    --  frmdata.logs.info('RES_CAP_SRC load from SNP to SRC is completed');
  
    frmdata.logs.info('RES_RTE_ASN_DTL_SRC load from SNP to SRC is completed');
  
  END LOAD_RES_RTE_ASN_DTLNN_SRC;

  ----------------------------------------------------------------------------------------------------------
  -- -----------------------------------------------------------------------------
  -- Load data into  RES_RTE_ASN_DTL_SRC table for records where alt_res_flg = 'Y'
  -- This procedure is used to load data for Sort and Test resources
  --
  -- -----------------------------------------------------------------------------

  PROCEDURE LOAD_RES_RTE_ASN_DTL_SRC(i_enterprise_nm IN VARCHAR2) IS
    --below declarations for ate--------
    ln_cnt_tester  INTEGER := 0;
    ln_cnt_handler INTEGER := 0;
    ln_cnt_hw_set  INTEGER := 0;
    -- lv_alt_route_flg  VARCHAR2(1) := 'N';
    --lv_alt_rsrc_flg   VARCHAR2(1) := 'N';
    lv_rsrc_type VARCHAR2(64);
    -- lv_hardware_type  VARCHAR2(64) ;
    -- lv_alt_res_id     VARCHAR2(256):= NULL;
    -- lv_alt_rte_res_id VARCHAR2(128):= NULL;
    -- lv_alt_res_id_cur VARCHAR2(256):= NULL;
    lv_prod_rte_seq_id RES_PART_ASN_SRC.PROD_RTE_SEQ_ID%TYPE;
    lv_prim_res_set_id RES_PART_ASN_SRC.PRIM_RES_SET_ID%TYPE;
  
    --Use the clone table scmdata.ROUTING_RES_ALT_SRC table populated to run the logic to populate
    -- RES_RTE_ASN_DTL_SRC where res_flg = 'Y'
  
    CURSOR cur_res_alt_src IS
      select ENTERPRISE,
             ENG_RTE_ID,
             PROD_RTE_SEQ_ID,
             PRIM_RES_SET_ID,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             RES_AREA,
             ETL_DT,
             ALT_RTE_FLG,
             ALT_RES_FLG
        from scmdata.RES_RTE_ASN_SRC
       WHERE ALT_RES_FLG = 'Y'
         AND RES_AREA <> 'NON ATE FT'
         and enterprise = i_enterprise_nm -- Added by VS 01/11/18
       ORDER BY ENG_RTE_ID, ENG_OPER_SEQ;
  
    -- CURSOR to get resource_set_ids for a given prim_resource_set_id from the res_attr_src table
  
    CURSOR cur_res_attr_src IS
      select distinct res_set_id
        from res_attr_src
       where prod_rte_seq_id = lv_prod_rte_seq_id
         and prim_res_set_id = lv_prim_res_set_id
         and prim_res_set_id != res_set_id;
  
    curval_res_alt  cur_res_alt_src%ROWTYPE;
    curval_res_attr cur_res_attr_src%ROWTYPE;
  
    -------below declarations for non ate---------
    ln_cnt_oven     INTEGER := 0;
    ln_cnt_board    INTEGER := 0;
    lv_rsrc_type_na VARCHAR2(64);
  
    lv_prod_rte_seq_id_na RES_PART_ASN_SRC.PROD_RTE_SEQ_ID%TYPE;
    lv_prim_res_set_id_na RES_PART_ASN_SRC.PRIM_RES_SET_ID%TYPE;
    lv_routingid_na       RES_RTE_ASN_SRC.ENG_RTE_ID%TYPE := NULL;
  
    CURSOR cur_res_alt_src_na IS
      select ENTERPRISE,
             ENG_RTE_ID,
             PROD_RTE_SEQ_ID,
             PRIM_RES_SET_ID,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             res_area,
             ETL_DT,
             ALT_RTE_FLG,
             ALT_RES_FLG
        from scmdata.RES_RTE_ASN_SRC
       WHERE ALT_RES_FLG = 'Y'
         and res_area = 'NON ATE FT'
         and enterprise = i_enterprise_nm -- Added by VS 01/11/18
       ORDER BY ENG_RTE_ID, ENG_OPER_SEQ;
  
    CURSOR cur_res_attr_src_na IS
      select distinct res_set_id
        from res_nonate_attr_src
       where prod_rte_seq_id = lv_prod_rte_seq_id_na
         and prim_res_set_id = lv_prim_res_set_id_na
         and prim_res_set_id != res_set_id
         and enterprise = i_enterprise_nm -- Added by VS 01/11/18
      ;
    curval_res_alt_na_na  cur_res_alt_src_na%ROWTYPE;
    curval_res_attr_na_na cur_res_attr_src_na%ROWTYPE;
  
  BEGIN
  
    -- lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date'23-May-2017';
  
    frmdata.logs.begin_log;
    --   frmdata.logs.info('Params:' || i_enterprise_nm);
    --frmdata.delete_data('RES_RTE_ASN_DTL_SRC', NULL, NULL);
  
    lv_row_source := 'V001';
  
    ----below logic is for ATE---------
  
    FOR curval_res_alt IN cur_res_alt_src LOOP
    
      ln_cnt_tester  := 0;
      ln_cnt_handler := 0;
      ln_cnt_hw_set  := 0;
    
      lv_rsrc_type := NULL;
    
      lv_prod_rte_seq_id := curval_res_alt.prod_rte_seq_id;
      lv_prim_res_set_id := curval_res_alt.prim_res_set_id;
    
      --  lv_row_source := lv_row_source ||'|A001';
    
      FOR curval_res_attr IN cur_res_attr_src LOOP
      
        -- For a given resource_set_id find if any Tester/Handler/Hardware set id is different than the primary*/
      
        select count(res_nm)
          INTO ln_cnt_tester
          from res_attr_src res
         where --prod_route_seq_id = 1027 and prim_resource_set_id = 2036
         prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
         and prim_res_set_id = curval_res_alt.prim_res_set_id
         and res_type = 'TESTER'
        -- and   prim_resource_set_id != resource_set_id
         and res_set_id = curval_res_attr.res_set_id
         and nvl(res_nm, 'dflt') !=
         (select nvl(res_nm, 'dflt')
            from res_attr_src pri
           where pri.prod_rte_seq_id = res.prod_rte_seq_id
             and pri.prim_res_set_id = res.prim_res_set_id
                --where pri.prod_route_seq_id = 1027 and pri.prim_resource_set_id = 2036
             and pri.res_type = 'TESTER'
             and pri.res_set_id = res.prim_res_set_id);
      
        select count(res_nm)
          INTO ln_cnt_handler
          from res_attr_src res
         where prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
           and prim_res_set_id = curval_res_alt.prim_res_set_id
           and res_type = 'HANDLER'
              -- and   prim_resource_set_id != resource_set_id
           and res_set_id = curval_res_attr.res_set_id
           and nvl(res_nm, 'dflt') !=
               (select nvl(res_nm, 'dflt')
                  from res_attr_src pri
                 where pri.prod_rte_seq_id = res.prod_rte_seq_id
                   and pri.prim_res_set_id = res.prim_res_set_id
                   and pri.res_type = 'HANDLER'
                   and pri.res_set_id = res.prim_res_set_id);
      
        select count(*)
          INTO ln_cnt_hw_set
          from (select distinct hw_type ALT_RES_Resource_TYPE,
                                hw_nm   ALT_RES_Resource_nm
                  from res_attr_src sec
                 where prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
                   and prim_res_set_id = curval_res_alt.prim_res_set_id
                   and res_type = 'HARDWARE_SET'
                      -- and   prim_resource_set_id != resource_set_id
                   and res_set_id = curval_res_attr.res_set_id
                MINUS
                select hw_type ALT_RES_Resource_TYPE,
                       hw_nm   ALT_RES_Resource_nm
                  from res_attr_src pri
                 where prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
                   and prim_res_set_id = curval_res_alt.prim_res_set_id
                   and res_type = 'HARDWARE_SET'
                   and prim_res_set_id = res_set_id);
      
        -- If only one resource is  different set alt resource as 'Y' keep checking if any other resources are different for the primary resource set
        -- and populate column alt_res_id
      
        IF (ln_cnt_tester > 0 AND ln_cnt_handler = 0 AND ln_cnt_hw_set = 0) THEN
          lv_rsrc_type := 'TESTER';
        
          INSERT INTO RES_RTE_ASN_DTL_SRC
            (ENTERPRISE,
             ENG_RTE_ID,
             prod_rte_seq_id,
             prim_res_set_id,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             RES_AREA,
             ALT_RTE_FLG,
             ALT_RES_FLG,
             ALT_RES_TYPE,
             ALT_RES_SET_ID,
             ALT_RES_NM,
             ALT_RES_PRIM_RES_NM,
             ALT_RES_UPH,
             SOURCE_DT,
             ETL_DT,
             ROW_SOURCE)
            select curval_res_alt.ENTERPRISE,
                   curval_res_alt.ENG_RTE_ID,
                   curval_res_alt.prod_rte_seq_id,
                   curval_res_alt.prim_res_set_id,
                   curval_res_alt.RTE_SEQ_NUM,
                   curval_res_alt.ENG_OPER_SEQ,
                   curval_res_alt.Res_Area,
                   curval_res_alt.ALT_RTE_FLG,
                   curval_res_alt.ALT_RES_FLG,
                   alt.res_type alt_res_resource_type,
                   alt.res_set_id alt_resource_set_id,
                   alt.res_nm alt_res_resource_nm,
                   prim.res_nm alt_res_prim_resource_nm,
                   alt.uph alt_res_uph,
                   sysdate,
                   curval_res_alt.ETL_DT,
                   lv_row_source || '-' || '3'
              from RES_ATTR_SRC alt, RES_ATTR_SRC prim
             where alt.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
               and alt.prim_res_set_id = curval_res_alt.prim_res_set_id
               and alt.res_type = 'TESTER'
               and alt.res_set_id = curval_res_attr.res_set_id
               and prim.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
               and prim.prim_res_set_id = curval_res_alt.prim_res_set_id
               and prim.res_set_id = curval_res_alt.prim_res_set_id
               and prim.res_type = 'TESTER';
        
        ELSIF (ln_cnt_handler > 0 AND ln_cnt_tester = 0 AND
              ln_cnt_hw_set = 0) THEN
          lv_rsrc_type := 'HANDLER';
        
          INSERT INTO RES_RTE_ASN_DTL_SRC
            (ENTERPRISE,
             ENG_RTE_ID,
             prod_rte_seq_id,
             prim_res_set_id,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             RES_AREA,
             ALT_RTE_FLG,
             ALT_RES_FLG,
             ALT_RES_TYPE,
             ALT_RES_SET_ID,
             ALT_RES_NM,
             ALT_RES_PRIM_RES_NM,
             ALT_RES_UPH,
             SOURCE_DT,
             ETL_DT,
             ROW_SOURCE)
            select curval_res_alt.ENTERPRISE,
                   curval_res_alt.ENG_RTE_ID,
                   curval_res_alt.prod_rte_seq_id,
                   curval_res_alt.prim_res_set_id,
                   curval_res_alt.RTE_SEQ_NUM,
                   curval_res_alt.ENG_OPER_SEQ,
                   curval_res_alt.res_area,
                   curval_res_alt.ALT_RTE_FLG,
                   curval_res_alt.ALT_RES_FLG,
                   alt.res_type alt_res_resource_type,
                   alt.res_set_id alt_resource_set_id,
                   alt.res_nm alt_res_resource_nm,
                   prim.res_nm alt_res_prim_resource_nm,
                   alt.uph alt_res_uph,
                   sysdate,
                   curval_res_alt.ETL_DT,
                   lv_row_source || '-' || '4'
              from RES_ATTR_SRC alt, RES_ATTR_SRC prim
             where alt.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
               and alt.prim_res_set_id = curval_res_alt.prim_res_set_id
               and alt.res_type = 'HANDLER'
               and alt.res_set_id = curval_res_attr.res_set_id
               and prim.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
               and prim.prim_res_set_id = curval_res_alt.prim_res_set_id
               and prim.res_set_id = curval_res_alt.prim_res_set_id
               and prim.res_type = 'HANDLER';
        
        ELSIF (ln_cnt_hw_set > 0 AND ln_cnt_handler = 0 AND
              ln_cnt_tester = 0) THEN
          lv_rsrc_type := 'HARDWARE_SET';
        
          -- special handling for hw_set_ids as we could have multiple rows
        
          FOR hw_rec in (select hw_type, res_type --INTO  lv_hardware_type,lv_rsrc_type
                           FROM (select distinct hw_type,
                                                 hw_nm ALT_RES_Resource_nm,
                                                 res_type
                                   from res_attr_src sec
                                  where prod_rte_seq_id =
                                        curval_res_alt.prod_rte_seq_id
                                    and prim_res_set_id =
                                        curval_res_alt.prim_res_set_id
                                    and res_type = 'HARDWARE_SET'
                                       -- and   prim_resource_set_id != resource_set_id
                                    and res_set_id =
                                        curval_res_attr.res_set_id
                                 MINUS
                                 select hw_type,
                                        hw_nm ALT_RES_Resource_nm,
                                        res_type
                                   from res_attr_src pri
                                  where prod_rte_seq_id =
                                        curval_res_alt.prod_rte_seq_id
                                    and prim_res_set_id =
                                        curval_res_alt.prim_res_set_id
                                    and res_type = 'HARDWARE_SET'
                                    and prim_res_set_id = res_set_id))
          
           LOOP
          
            --lv_hardware_type,lv_rsrc_type,
          
            INSERT INTO RES_RTE_ASN_DTL_SRC
              (ENTERPRISE,
               ENG_RTE_ID,
               prod_rte_seq_id,
               prim_res_set_id,
               RTE_SEQ_NUM,
               ENG_OPER_SEQ,
               res_area,
               ALT_RTE_FLG,
               ALT_RES_FLG,
               ALT_RES_TYPE,
               ALT_RES_SET_ID,
               ALT_RES_NM,
               ALT_RES_PRIM_RES_NM,
               ALT_RES_HW_SET_ID,
               ALT_RES_UPH,
               SOURCE_DT,
               ETL_DT,
               ROW_SOURCE)
              select curval_res_alt.ENTERPRISE,
                     curval_res_alt.ENG_RTE_ID,
                     curval_res_alt.prod_rte_seq_id,
                     curval_res_alt.prim_res_set_id,
                     curval_res_alt.RTE_SEQ_NUM,
                     curval_res_alt.ENG_OPER_SEQ,
                     curval_res_alt.res_area,
                     curval_res_alt.ALT_RTE_FLG,
                     curval_res_alt.ALT_RES_FLG,
                     alt.res_type || '_' || alt.hw_type alt_res_resource_type,
                     alt.res_set_id alt_resource_set_id,
                     alt.hw_nm alt_res_resource_nm,
                     prim.hw_nm alt_res_prim_resource_nm,
                     alt.res_nm alt_res_hw_set_id,
                     alt.uph,
                     sysdate,
                     curval_res_alt.ETL_DT,
                     lv_row_source || '-' || '5' alt_res_uph
                from RES_ATTR_SRC alt, RES_ATTR_SRC prim
               where alt.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
                 and alt.prim_res_set_id = curval_res_alt.prim_res_set_id
                 and alt.res_type = hw_rec.res_type -- lv_rsrc_type
                 and alt.hw_type = hw_rec.hw_type --lv_hardware_type
                 and alt.res_set_id = curval_res_attr.res_set_id
                 and prim.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
                 and prim.prim_res_set_id = curval_res_alt.prim_res_set_id
                 and prim.res_set_id = curval_res_alt.prim_res_set_id
                 and prim.res_type = hw_rec.res_type --lv_rsrc_type
                 and prim.hw_type = hw_rec.hw_type; --lv_hardware_type ;
          
          END LOOP;
        
        END IF;
      
      END LOOP;
    
      /* Adding data for rest of the resources that are not different in the exploded detail table */
    
      IF lv_rsrc_type = 'TESTER' THEN
      
        INSERT INTO RES_RTE_ASN_DTL_SRC
          (ENTERPRISE,
           ENG_RTE_ID,
           prod_rte_seq_id,
           prim_res_set_id,
           RTE_SEQ_NUM,
           ENG_OPER_SEQ,
           res_area,
           ALT_RTE_FLG,
           ALT_RES_FLG,
           ALT_RES_TYPE,
           ALT_RES_PRIM_RES_NM,
           ALT_RES_NM,
           ALT_RES_HW_SET_ID,
           ALT_RES_UPH,
           SOURCE_DT,
           ETL_DT,
           ROW_SOURCE)
          select curval_res_alt.ENTERPRISE,
                 curval_res_alt.ENG_RTE_ID,
                 curval_res_alt.prod_rte_seq_id,
                 curval_res_alt.prim_res_set_id,
                 curval_res_alt.RTE_SEQ_NUM,
                 curval_res_alt.ENG_OPER_SEQ,
                 curval_res_alt.res_area,
                 curval_res_alt.ALT_RTE_FLG,
                 curval_res_alt.ALT_RES_FLG,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    RES_TYPE || '_' || HW_TYPE
                   ELSE
                    prim.res_type
                 END alt_res_resource_type,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    hw_nm
                   ELSE
                    res_nm
                 END alt_res_prim_resource_nm,
                 NULL alt_res_resource_nm,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    res_nm
                   ELSE
                    NULL
                 END ALT_RES_HW_SET_ID,
                 prim.uph alt_res_uph,
                 sysdate,
                 curval_res_alt.ETL_DT,
                 lv_row_source || '-' || '6'
            FROM RES_ATTR_SRC prim
           where prim.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
             and prim.prim_res_set_id = curval_res_alt.prim_res_set_id
             and prim.res_set_id = curval_res_alt.prim_res_set_id
             and prim.res_type IN ('HANDLER', 'HARDWARE_SET');
      
      ELSIF lv_rsrc_type = 'HANDLER' THEN
      
        INSERT INTO RES_RTE_ASN_DTL_SRC
          (ENTERPRISE,
           ENG_RTE_ID,
           prod_rte_seq_id,
           prim_res_set_id,
           RTE_SEQ_NUM,
           ENG_OPER_SEQ,
           res_area,
           ALT_RTE_FLG,
           ALT_RES_FLG,
           ALT_RES_TYPE,
           ALT_RES_PRIM_RES_NM,
           ALT_RES_NM,
           ALT_RES_HW_SET_ID,
           ALT_RES_UPH,
           SOURCE_DT,
           ETL_DT,
           ROW_SOURCE)
          select curval_res_alt.ENTERPRISE,
                 curval_res_alt.ENG_RTE_ID,
                 curval_res_alt.prod_rte_seq_id,
                 curval_res_alt.prim_res_set_id,
                 curval_res_alt.RTE_SEQ_NUM,
                 curval_res_alt.ENG_OPER_SEQ,
                 curval_res_alt.res_area,
                 curval_res_alt.ALT_RTE_FLG,
                 curval_res_alt.ALT_RES_FLG,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    RES_TYPE || '_' || HW_TYPE
                   ELSE
                    prim.res_type
                 END alt_res_resource_type,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    hw_nm
                   ELSE
                    res_nm
                 END alt_res_prim_resource_nm,
                 NULL alt_res_resource_nm,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    res_nm
                   ELSE
                    NULL
                 END ALT_RES_HW_SET_ID,
                 prim.uph alt_res_uph,
                 sysdate,
                 curval_res_alt.ETL_DT,
                 lv_row_source || '-' || '7'
            FROM RES_ATTR_SRC prim
           where prim.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
             and prim.prim_res_set_id = curval_res_alt.prim_res_set_id
             and prim.res_set_id = curval_res_alt.prim_res_set_id
             and prim.res_type IN ('TESTER', 'HARDWARE_SET');
      
      ELSIF lv_rsrc_type = 'HARDWARE_SET' THEN
      
        INSERT INTO RES_RTE_ASN_DTL_SRC
          (ENTERPRISE,
           ENG_RTE_ID,
           prod_rte_seq_id,
           prim_res_set_id,
           RTE_SEQ_NUM,
           ENG_OPER_SEQ,
           res_area,
           ALT_RTE_FLG,
           ALT_RES_FLG,
           ALT_RES_TYPE,
           ALT_RES_PRIM_RES_NM,
           ALT_RES_NM,
           ALT_RES_HW_SET_ID,
           ALT_RES_UPH,
           SOURCE_DT,
           ETL_DT,
           ROW_SOURCE)
          select curval_res_alt.ENTERPRISE,
                 curval_res_alt.ENG_RTE_ID,
                 curval_res_alt.prod_rte_seq_id,
                 curval_res_alt.prim_res_set_id,
                 curval_res_alt.RTE_SEQ_NUM,
                 curval_res_alt.ENG_OPER_SEQ,
                 curval_res_alt.res_area,
                 curval_res_alt.ALT_RTE_FLG,
                 curval_res_alt.ALT_RES_FLG,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    RES_TYPE || '_' || HW_TYPE
                   ELSE
                    prim.res_type
                 END alt_res_resource_type,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    hw_nm
                   ELSE
                    res_nm
                 END alt_res_prim_resource_nm,
                 NULL alt_res_resource_nm,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    res_nm
                   ELSE
                    NULL
                 END ALT_RES_HW_SET_ID,
                 prim.uph alt_res_uph,
                 sysdate,
                 curval_res_alt.ETL_DT,
                 lv_row_source || '-' || '8'
            FROM RES_ATTR_SRC prim
           where prim.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
             and prim.prim_res_set_id = curval_res_alt.prim_res_set_id
             and prim.res_set_id = curval_res_alt.prim_res_set_id
             and (prim.res_type = 'TESTER' OR prim.res_type = 'HANDLER')
          UNION
          SELECT curval_res_alt.ENTERPRISE,
                 curval_res_alt.ENG_RTE_ID,
                 curval_res_alt.prod_rte_seq_id,
                 curval_res_alt.prim_res_set_id,
                 curval_res_alt.RTE_SEQ_NUM,
                 curval_res_alt.ENG_OPER_SEQ,
                 curval_res_alt.res_area,
                 curval_res_alt.ALT_RTE_FLG,
                 curval_res_alt.ALT_RES_FLG,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    RES_TYPE || '_' || HW_TYPE
                   ELSE
                    prim.res_type
                 END alt_res_resource_type,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    hw_nm
                   ELSE
                    res_nm
                 END alt_res_prim_resource_nm,
                 NULL alt_res_resource_nm,
                 CASE
                   WHEN prim.res_type = 'HARDWARE_SET' THEN
                    res_nm
                   ELSE
                    NULL
                 END ALT_RES_HW_SET_ID,
                 prim.uph alt_res_uph,
                 sysdate,
                 curval_res_alt.ETL_DT,
                 lv_row_source || '-' || '9'
            FROM RES_ATTR_SRC prim
           where prim.prod_rte_seq_id = curval_res_alt.prod_rte_seq_id
             and prim.prim_res_set_id = curval_res_alt.prim_res_set_id
             and prim.res_set_id = curval_res_alt.prim_res_set_id
                --and (prim.resource_type = 'HW_SET_ID' AND prim.hardware_type  != lv_hardware_type);*\
             and (prim.res_type = 'HARDWARE_SET' AND
                 prim.hw_type not in
                 (select hw_type
                     FROM (select distinct hw_type,
                                           hw_nm ALT_RES_Resource_nm,
                                           res_type
                             from res_attr_src sec
                            where prod_rte_seq_id =
                                  curval_res_alt.prod_rte_seq_id
                              and prim_res_set_id =
                                  curval_res_alt.prim_res_set_id
                              and res_type = 'HARDWARE_SET'
                                 -- and   prim_resource_set_id != resource_set_id
                              and res_set_id =
                                  (select distinct res_set_id
                                     from RES_ATTR_SRC
                                    where prod_rte_seq_id =
                                          curval_res_alt.prod_rte_seq_id
                                      and prim_res_set_id =
                                          curval_res_alt.prim_res_set_id
                                      and res_set_id != prim_res_set_id
                                      and rownum < 2
                                   
                                   )
                           --  curval_res_attr.resource_set_id
                           MINUS
                           select distinct hw_type,
                                           hw_nm ALT_RES_Resource_nm,
                                           res_type
                             from res_attr_src pri
                            where prod_rte_seq_id =
                                  curval_res_alt.prod_rte_seq_id
                              and prim_res_set_id =
                                  curval_res_alt.prim_res_set_id
                              and res_type = 'HARDWARE_SET'
                              and prim_res_set_id = res_set_id)));
      END IF;
    
    END LOOP;
  
    --------below logic is for NON ATE----------
  
    FOR curval_res_alt_na IN cur_res_alt_src_na LOOP
    
      ln_cnt_oven  := 0;
      ln_cnt_board := 0;
    
      lv_rsrc_type_na := NULL;
    
      lv_prod_rte_seq_id_na := curval_res_alt_na.prod_rte_seq_id;
      lv_prim_res_set_id_na := curval_res_alt_na.prim_res_set_id;
    
      FOR curval_res_attr_na IN cur_res_attr_src_na LOOP
        ------cnt for ovens
        select count(res_nm)
          into ln_cnt_oven
          from res_nonate_attr_src res
         where prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
           and prim_res_set_id = curval_res_alt_na.prim_res_set_id
           and res_type = 'BIOV'
           and res_set_id = curval_res_attr_na.res_set_id
           and enterprise = i_enterprise_nm -- Added by VS 01/11/18
           and nvl(res_nm, 'dflt') <>
               (select nvl(res_nm, 'dflt')
                  from res_nonate_attr_src pri
                 where pri.prod_rte_seq_id = res.prod_rte_seq_id
                   and pri.prim_res_set_id = res.prim_res_set_id
                   and pri.res_type = 'BIOV'
                   and enterprise = i_enterprise_nm -- Added by VS 01/11/18
                   and pri.res_set_id = res.prim_res_set_id);
        -----cnt for boards
        select count(res_nm)
          into ln_cnt_board
          from res_nonate_attr_src res
         where prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
           and prim_res_set_id = curval_res_alt_na.prim_res_set_id
           and res_type = 'BIBR'
           and res_set_id = curval_res_attr_na.res_set_id
           AND enterprise = i_enterprise_nm --added by sa on 1/10
           and nvl(res_nm, 'dflt') <>
               (select nvl(res_nm, 'dflt')
                  from res_nonate_attr_src pri
                 where pri.prod_rte_seq_id = res.prod_rte_seq_id
                   and pri.prim_res_set_id = res.prim_res_set_id
                   AND pri.enterprise = res.enterprise --added by sa on 1/10
                   and pri.res_type = 'BIBR'
                   and pri.res_set_id = res.prim_res_set_id);
      
        if (ln_cnt_oven > 0 and ln_cnt_board = 0) then
          lv_rsrc_type_na := 'BIOV';
        
          INSERT INTO RES_RTE_ASN_DTL_SRC
            (ENTERPRISE,
             ENG_RTE_ID,
             prod_rte_seq_id,
             prim_res_set_id,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             res_area,
             ALT_RTE_FLG,
             ALT_RES_FLG,
             ALT_RES_TYPE,
             ALT_RES_SET_ID,
             ALT_RES_NM,
             ALT_RES_PRIM_RES_NM,
             -- ALT_RES_UPH,
             SOURCE_DT,
             ETL_DT,
             ROW_SOURCE)
            select curval_res_alt_na.ENTERPRISE,
                   curval_res_alt_na.ENG_RTE_ID,
                   curval_res_alt_na.prod_rte_seq_id,
                   curval_res_alt_na.prim_res_set_id,
                   curval_res_alt_na.RTE_SEQ_NUM,
                   curval_res_alt_na.ENG_OPER_SEQ,
                   curval_res_alt_na.res_area,
                   curval_res_alt_na.ALT_RTE_FLG,
                   curval_res_alt_na.ALT_RES_FLG,
                   alt.res_type                      alt_res_resource_type,
                   alt.res_set_id                    alt_resource_set_id,
                   alt.res_nm                        alt_res_resource_nm,
                   prim.res_nm                       alt_res_prim_resource_nm,
                   -- alt.uph                        alt_res_uph,
                   sysdate,
                   curval_res_alt_na.ETL_DT,
                   lv_row_source || '-' || '10'
              from RES_nonate_ATTR_SRC alt, RES_nonate_ATTR_SRC prim
             where alt.prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
               and alt.prim_res_set_id = curval_res_alt_na.prim_res_set_id
               and alt.res_type = 'BIOV'
               and alt.res_set_id = curval_res_attr_na.res_set_id
               and prim.prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
               and prim.prim_res_set_id = curval_res_alt_na.prim_res_set_id
               and prim.res_set_id = curval_res_alt_na.prim_res_set_id
               AND alt.enterprise = i_enterprise_nm --added by SA on 1/10
               AND alt.enterprise = prim.enterprise --added by SA on 1/10
                  
               and prim.res_type = 'BIOV';
        
        elsif (ln_cnt_board > 0 and ln_cnt_oven = 0) then
          lv_rsrc_type_na := 'BIBR';
          INSERT INTO RES_RTE_ASN_DTL_SRC
            (ENTERPRISE,
             ENG_RTE_ID,
             prod_rte_seq_id,
             prim_res_set_id,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             res_area,
             ALT_RTE_FLG,
             ALT_RES_FLG,
             ALT_RES_TYPE,
             ALT_RES_SET_ID,
             ALT_RES_NM,
             ALT_RES_PRIM_RES_NM,
             --ALT_RES_UPH,
             SOURCE_DT,
             ETL_DT,
             ROW_SOURCE)
            select curval_res_alt_na.ENTERPRISE,
                   curval_res_alt_na.ENG_RTE_ID,
                   curval_res_alt_na.prod_rte_seq_id,
                   curval_res_alt_na.prim_res_set_id,
                   curval_res_alt_na.RTE_SEQ_NUM,
                   curval_res_alt_na.ENG_OPER_SEQ,
                   curval_res_alt_na.res_area,
                   curval_res_alt_na.ALT_RTE_FLG,
                   curval_res_alt_na.ALT_RES_FLG,
                   alt.res_type                      alt_res_resource_type,
                   alt.res_set_id                    alt_resource_set_id,
                   alt.res_nm                        alt_res_resource_nm,
                   prim.res_nm                       alt_res_prim_resource_nm,
                   --alt.uph                        alt_res_uph,
                   sysdate,
                   curval_res_alt_na.ETL_DT,
                   lv_row_source || '-' || '11'
              from RES_nonate_ATTR_SRC alt, RES_nonate_ATTR_SRC prim
             where alt.prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
               and alt.prim_res_set_id = curval_res_alt_na.prim_res_set_id
               and alt.res_type = 'BIBR'
               and alt.res_set_id = curval_res_attr_na.res_set_id
               and prim.prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
               and prim.prim_res_set_id = curval_res_alt_na.prim_res_set_id
               and prim.res_set_id = curval_res_alt_na.prim_res_set_id
               AND alt.enterprise = i_enterprise_nm --added by SA on 1/10
               AND alt.enterprise = prim.enterprise --added by SA on 1/10
               and prim.res_type = 'BIBR';
        
        end if;
      
      end loop;
    
      IF lv_rsrc_type_na = 'BIOV' THEN
      
        INSERT INTO RES_RTE_ASN_DTL_SRC
          (ENTERPRISE,
           ENG_RTE_ID,
           prod_rte_seq_id,
           prim_res_set_id,
           RTE_SEQ_NUM,
           ENG_OPER_SEQ,
           res_area,
           ALT_RTE_FLG,
           ALT_RES_FLG,
           ALT_RES_TYPE,
           ALT_RES_PRIM_RES_NM,
           ALT_RES_NM,
           ALT_RES_HW_SET_ID,
           --  ALT_RES_UPH,
           SOURCE_DT,
           ETL_DT,
           ROW_SOURCE)
          select curval_res_alt_na.ENTERPRISE,
                 curval_res_alt_na.ENG_RTE_ID,
                 curval_res_alt_na.prod_rte_seq_id,
                 curval_res_alt_na.prim_res_set_id,
                 curval_res_alt_na.RTE_SEQ_NUM,
                 curval_res_alt_na.ENG_OPER_SEQ,
                 curval_res_alt_na.res_area,
                 curval_res_alt_na.ALT_RTE_FLG,
                 curval_res_alt_na.ALT_RES_FLG,
                 prim.res_type                     alt_res_resource_type,
                 prim.res_nm                       alt_res_prim_resource_nm,
                 NULL                              alt_res_resource_nm,
                 prim.res_nm                       ALT_RES_HW_SET_ID,
                 -- prim.uph alt_res_uph,
                 sysdate,
                 curval_res_alt_na.ETL_DT,
                 lv_row_source || '-' || '13'
            FROM RES_nonate_ATTR_SRC prim
           where prim.prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
             and prim.prim_res_set_id = curval_res_alt_na.prim_res_set_id
             and prim.res_set_id = curval_res_alt_na.prim_res_set_id
             AND prim.enterprise = i_enterprise_nm --added by SA on 1/10
             and prim.res_type = 'BIBR';
      
      ELSIF lv_rsrc_type_na = 'BIBR' THEN
      
        INSERT INTO RES_RTE_ASN_DTL_SRC
          (ENTERPRISE,
           ENG_RTE_ID,
           prod_rte_seq_id,
           prim_res_set_id,
           RTE_SEQ_NUM,
           ENG_OPER_SEQ,
           res_area,
           ALT_RTE_FLG,
           ALT_RES_FLG,
           ALT_RES_TYPE,
           ALT_RES_PRIM_RES_NM,
           ALT_RES_NM,
           ALT_RES_HW_SET_ID,
           --ALT_RES_UPH,
           SOURCE_DT,
           ETL_DT,
           ROW_SOURCE)
          select curval_res_alt_na.ENTERPRISE,
                 curval_res_alt_na.ENG_RTE_ID,
                 curval_res_alt_na.prod_rte_seq_id,
                 curval_res_alt_na.prim_res_set_id,
                 curval_res_alt_na.RTE_SEQ_NUM,
                 curval_res_alt_na.ENG_OPER_SEQ,
                 curval_res_alt_na.res_area,
                 curval_res_alt_na.ALT_RTE_FLG,
                 curval_res_alt_na.ALT_RES_FLG,
                 prim.res_type                     alt_res_resource_type,
                 res_nm                            alt_res_prim_resource_nm,
                 null                              ALT_RES_HW_SET_ID,
                 null,
                 -- prim.uph alt_res_uph,
                 sysdate,
                 curval_res_alt_na.ETL_DT,
                 lv_row_source || '-' || '14'
            FROM RES_nonate_ATTR_SRC prim
           where prim.prod_rte_seq_id = curval_res_alt_na.prod_rte_seq_id
             and prim.prim_res_set_id = curval_res_alt_na.prim_res_set_id
             and prim.res_set_id = curval_res_alt_na.prim_res_set_id
             AND prim.enterprise = i_enterprise_nm --added by SA on 1/10
             and prim.res_type = 'BIOV';
      
      end if;
    
    end loop;
  
    commit;
  
    frmdata.logs.info('RES_RTE_ASN_DTL_SRC load from SNP to SRC is completed');
  
  END LOAD_RES_RTE_ASN_DTL_SRC;

  ----------------------------------------------------------------------------------------------------------
  --************************************************************
  --Procedure to load data into ROUTE_RES_ALT_DTL_SRC
  -- alternate routes for rows with alt_rte_flg = 'Y'
  -- This procedure is used to load data for Sort and Test resources
  --************************************************************

  PROCEDURE LOAD_ROUTE_RES_ALT_DTLRTE_SRC(i_enterprise_nm IN VARCHAR2) IS
  
    /* We have the clone table scmdata.ROUTING_RES_ALT_SRC table populated now we need to run the logic to populate
    ROUTE_RES_ALT_DTL_SRC where rte_flg = 'Y'
    */
    ------below declarations are for ATE-----
  
    ln_cnt_engopseq    INTEGER := 0;
    ln_cnt_rte_res_set INTEGER := 0;
    lv_routingid       RES_RTE_ASN_SRC.ENG_RTE_ID%TYPE := NULL;
  
    CURSOR cur_altrte IS
      select distinct routeres.ENG_RTE_ID
        from scmdata.RES_RTE_ASN_SRC routeres
       WHERE routeres.ALT_RTE_FLG = 'Y'
         and routeres.res_area <> 'NON ATE FT'
            -- and routeres.eng_routingid = 'MAX3799ETJ+_T1_S_4900_1'
         and routeres.enterprise = i_enterprise_nm;
  
    CURSOR cur_altrte_attr IS
      select distinct resattr.res_set_id alt_rte_res_set_id
        from scmdata.RES_RTE_ASN_SRC routeres, res_attr_src resattr
       WHERE routeres.ALT_RTE_FLG = 'Y'
         and routeres.res_area <> 'NON ATE FT'
         and routeres.enterprise = i_enterprise_nm
         and routeres.prod_rte_seq_id = resattr.prod_rte_seq_id
         and routeres.prim_res_set_id = resattr.prim_res_set_id
         and resattr.prim_res_set_id != resattr.res_set_id
         and routeres.eng_rte_id = lv_routingid; -- 'MAX3799ETJ+_T1_S_4900_1';
  
    curval_altrte      cur_altrte%ROWTYPE;
    curval_altrte_attr cur_altrte_attr%ROWTYPE;
  
    ----------below declarations are for non ate----------
  
    ln_cnt_engopseq_na    INTEGER := 0;
    ln_cnt_rte_res_set_na INTEGER := 0;
    lv_routingid_na       RES_RTE_ASN_SRC.ENG_RTE_ID%TYPE := NULL;
  
    CURSOR cur_altrte_na IS
      select distinct routeres.ENG_RTE_ID
        from scmdata.RES_RTE_ASN_SRC routeres
       WHERE routeres.ALT_RTE_FLG = 'Y'
         and res_area = 'NON ATE FT'
         and routeres.enterprise = i_enterprise_nm;
  
    CURSOR cur_altrte_attr_na IS
      select distinct resattr.res_set_id alt_rte_res_set_id
        from scmdata.RES_RTE_ASN_SRC routeres, res_nonate_attr_src resattr
       WHERE routeres.ALT_RTE_FLG = 'Y'
         and routeres.enterprise = i_enterprise_nm
         AND routeres.enterprise = resattr.enterprise -- added to SA on 1/10
         and routeres.prod_rte_seq_id = resattr.prod_rte_seq_id
         and routeres.prim_res_set_id = resattr.prim_res_set_id
         and resattr.prim_res_set_id != resattr.res_set_id
         and routeres.res_area = 'NON ATE FT'
         and routeres.eng_rte_id = lv_routingid_na;
  
  BEGIN
  
    --lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date'23-May-2017';
  
    lv_row_source := 'W001';
  
    frmdata.logs.begin_log;
    --   frmdata.logs.info('Params:' || i_enterprise_nm);
    -- frmdata.delete_data('ROUTE_RES_ALT_DTL_SRC', NULL, NULL);
  
    FOR curval_altrte IN cur_altrte LOOP
    
      lv_routingid := curval_altrte.ENG_RTE_ID;
    
      -- ln_cnt_engopseq := 0;
    
      select count(distinct ENG_OPER_SEQ)
        INTO ln_cnt_engopseq
        from scmdata.RES_RTE_ASN_SRC routeres
       where routeres.eng_rte_id = lv_routingid
         and routeres.res_area <> 'NON ATE FT'
         and enterprise = i_enterprise_nm; -- 'MAX3799ETJ+_T1_S_4900_1';
    
      FOR curval_altrte_attr IN cur_altrte_attr LOOP
      
        select count(*)
          INTO ln_cnt_rte_res_set
          from (select distinct routeres.ENTERPRISE,
                                routeres.ENG_RTE_ID,
                                routeres.ENG_OPER_SEQ,
                                resattr.res_set_id alt_rte_res_set_id
                  from scmdata.RES_RTE_ASN_SRC routeres,
                       res_attr_src            resattr
                 WHERE routeres.ALT_RTE_FLG = 'Y'
                   and routeres.enterprise = i_enterprise_nm --'SCP_DAILY' --
                   and routeres.res_area <> 'NON ATE FT'
                   and routeres.prod_rte_seq_id = resattr.prod_rte_seq_id
                   and routeres.prim_res_set_id = resattr.prim_res_set_id
                   and resattr.prim_res_set_id != resattr.res_set_id
                   and routeres.eng_rte_id = lv_routingid)
         where alt_rte_res_set_id = curval_altrte_attr.alt_rte_res_set_id;
      
        IF ln_cnt_rte_res_set = ln_cnt_engopseq THEN
        
          INSERT INTO RES_RTE_ASN_DTL_SRC
            (ENTERPRISE,
             ENG_RTE_ID,
             prod_rte_seq_id,
             PRIM_RES_SET_ID,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             ALT_RTE_FLG,
             ALT_RES_FLG,
             ALT_RTE_RES_SET_ID,
             RES_AREA,
             SOURCE_DT,
             ETL_DT,
             ROW_SOURCE)
            select distinct routeres.ENTERPRISE,
                            routeres.ENG_RTE_ID,
                            routeres.prod_rte_seq_id,
                            routeres.PRIM_RES_SET_ID,
                            routeres.RTE_SEQ_NUM,
                            routeres.ENG_OPER_SEQ,
                            routeres.ALT_RTE_FLG,
                            routeres.ALT_RES_FLG,
                            resattr.res_set_id alt_rte_res_set_id,
                            routeres.res_area,
                            sysdate,
                            routeres.ETL_DT,
                            lv_row_source || '-' || '1'
              from scmdata.RES_RTE_ASN_SRC routeres, res_attr_src resattr
             WHERE routeres.ALT_RTE_FLG = 'Y'
               and routeres.enterprise = i_enterprise_nm
               and routeres.res_area <> 'NON ATE FT'
               and routeres.prod_rte_seq_id = resattr.prod_rte_seq_id(+)
               and routeres.prim_res_set_id = resattr.prim_res_set_id(+)
               and routeres.eng_rte_id = lv_routingid
               and resattr.res_set_id =
                   curval_altrte_attr.alt_rte_res_set_id;
        
        ELSE
        
          INSERT INTO RES_RTE_ASN_DTL_SRC
            (ENTERPRISE,
             ENG_RTE_ID,
             prod_rte_seq_id,
             PRIM_RES_SET_ID,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             ALT_RTE_FLG,
             ALT_RES_FLG,
             ALT_RTE_RES_SET_ID,
             RES_AREA, --added by SA on 1/9
             SOURCE_DT,
             ETL_DT,
             ROW_SOURCE)
            select distinct routeres.ENTERPRISE,
                            routeres.ENG_RTE_ID,
                            routeres.PROD_RTE_SEQ_ID,
                            routeres.PRIM_RES_SET_ID,
                            routeres.RTE_SEQ_NUM,
                            routeres.ENG_OPER_SEQ,
                            routeres.ALT_RTE_FLG,
                            routeres.ALT_RES_FLG,
                            resattr.res_set_id alt_rte_res_set_id,
                            routeres.res_area,
                            sysdate,
                            routeres.ETL_DT,
                            lv_row_source || '-' || '15'
              FROM scmdata.RES_RTE_ASN_SRC routeres, res_attr_src resattr
             WHERE routeres.ALT_RTE_FLG = 'Y'
               AND routeres.enterprise = i_enterprise_nm
               and routeres.res_area <> 'NON ATE FT'
               AND routeres.prod_rte_seq_id = resattr.prod_rte_seq_id
               AND routeres.prim_res_set_id = resattr.prim_res_set_id
                  -- and resattr.prim_resource_set_id != resattr.resource_set_id
               AND routeres.eng_rte_id = lv_routingid -- 'MAX3799ETJ+_T1_S_4900_1'
               AND resattr.res_set_id =
                   curval_altrte_attr.alt_rte_res_set_id
            UNION
            SELECT distinct routeres.ENTERPRISE,
                            routeres.ENG_RTE_ID,
                            routeres.PROD_RTE_SEQ_ID,
                            routeres.PRIM_RES_SET_ID,
                            routeres.RTE_SEQ_NUM,
                            routeres.ENG_OPER_SEQ,
                            routeres.ALT_RTE_FLG,
                            routeres.ALT_RES_FLG,
                            routeres.PRIM_RES_SET_ID alt_rte_res_set_id,
                            routeres.res_area,
                            sysdate,
                            routeres.ETL_DT,
                            lv_row_source || '-' || '15'
            --    lv_row_source
              from scmdata.RES_RTE_ASN_SRC routeres
             WHERE routeres.ALT_RTE_FLG = 'Y'
               and routeres.enterprise = i_enterprise_nm
               and routeres.res_area <> 'NON ATE FT'
               and routeres.eng_rte_id = lv_routingid
               and routeres.eng_oper_seq not in
                   (select distinct routeres.ENG_OPER_SEQ
                      from scmdata.RES_RTE_ASN_SRC routeres,
                           res_attr_src            resattr
                     WHERE routeres.ALT_RTE_FLG = 'Y'
                       and routeres.res_area <> 'NON ATE FT'
                       and routeres.enterprise = i_enterprise_nm
                       and routeres.prod_rte_seq_id =
                           resattr.prod_rte_seq_id(+)
                       and routeres.prim_res_set_id =
                           resattr.prim_res_set_id(+)
                          -- and resattr.prim_resource_set_id != resattr.resource_set_id
                       and routeres.eng_rte_id = lv_routingid --'MAX3799ETJ+_T1_S_4900_1'
                       and resattr.res_set_id =
                           curval_altrte_attr.alt_rte_res_set_id);
        
          --  dbms_output.put_line('Make up data');
        
          commit;
        
        END IF;
      
      END LOOP;
    
    END LOOP;
  
    ---------below logic is for NON ATE---------
    FOR curval_altrte_na IN cur_altrte_na LOOP
      lv_routingid_na := curval_altrte_na.ENG_RTE_ID;
    
      select count(distinct ENG_OPER_SEQ)
        INTO ln_cnt_engopseq_na
        from scmdata.RES_RTE_ASN_SRC routeres
       where routeres.eng_rte_id = lv_routingid_na
         and enterprise = i_enterprise_nm ----Added by VS 01/11/18
         and res_area = 'NON ATE FT';
    
      FOR curval_altrte_attr_na IN cur_altrte_attr_na LOOP
        select count(*)
          INTO ln_cnt_rte_res_set_na
          from (select distinct routeres.ENTERPRISE,
                                routeres.ENG_RTE_ID,
                                routeres.ENG_OPER_SEQ,
                                resattr.res_set_id alt_rte_res_set_id
                  from scmdata.RES_RTE_ASN_SRC routeres,
                       res_nonate_attr_src     resattr
                 WHERE routeres.ALT_RTE_FLG = 'Y'
                   and routeres.enterprise = i_enterprise_nm
                   and routeres.prod_rte_seq_id = resattr.prod_rte_seq_id
                   and routeres.prim_res_set_id = resattr.prim_res_set_id
                   and resattr.prim_res_set_id != resattr.res_set_id
                   and routeres.res_area = 'NON ATE FT'
                   and routeres.eng_rte_id = lv_routingid_na)
         where alt_rte_res_set_id =
               curval_altrte_attr_na.alt_rte_res_set_id;
        IF ln_cnt_rte_res_set_na = ln_cnt_engopseq_na THEN
        
          INSERT INTO RES_RTE_ASN_DTL_SRC
            (ENTERPRISE,
             ENG_RTE_ID,
             prod_rte_seq_id,
             PRIM_RES_SET_ID,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             res_area,
             ALT_RTE_FLG,
             ALT_RES_FLG,
             ALT_RTE_RES_SET_ID,
             SOURCE_DT,
             ETL_DT,
             ROW_SOURCE)
            select distinct routeres.ENTERPRISE,
                            routeres.ENG_RTE_ID,
                            routeres.prod_rte_seq_id,
                            routeres.PRIM_RES_SET_ID,
                            routeres.RTE_SEQ_NUM,
                            routeres.ENG_OPER_SEQ,
                            routeres.res_area,
                            routeres.ALT_RTE_FLG,
                            routeres.ALT_RES_FLG,
                            resattr.res_set_id alt_rte_res_set_id,
                            sysdate,
                            routeres.ETL_DT,
                            lv_row_source || '-' || '16'
              from scmdata.RES_RTE_ASN_SRC routeres,
                   res_nonate_attr_src     resattr
             WHERE routeres.ALT_RTE_FLG = 'Y'
               and routeres.enterprise = i_enterprise_nm
               and routeres.prod_rte_seq_id = resattr.prod_rte_seq_id(+)
               and routeres.prim_res_set_id = resattr.prim_res_set_id(+)
               and routeres.res_area = 'NON ATE FT'
               and routeres.eng_rte_id = lv_routingid_na
               and resattr.res_set_id =
                   curval_altrte_attr_na.alt_rte_res_set_id;
        
        ELSE
        
          INSERT INTO RES_RTE_ASN_DTL_SRC
            (ENTERPRISE,
             ENG_RTE_ID,
             prod_rte_seq_id,
             PRIM_RES_SET_ID,
             RTE_SEQ_NUM,
             ENG_OPER_SEQ,
             res_area,
             ALT_RTE_FLG,
             ALT_RES_FLG,
             ALT_RTE_RES_SET_ID,
             SOURCE_DT,
             ETL_DT,
             ROW_SOURCE)
            select distinct routeres.ENTERPRISE,
                            routeres.ENG_RTE_ID,
                            routeres.PROD_RTE_SEQ_ID,
                            routeres.PRIM_RES_SET_ID,
                            routeres.RTE_SEQ_NUM,
                            routeres.ENG_OPER_SEQ,
                            routeres.res_area,
                            routeres.ALT_RTE_FLG,
                            routeres.ALT_RES_FLG,
                            resattr.res_set_id alt_rte_res_set_id,
                            sysdate,
                            routeres.ETL_DT,
                            lv_row_source || '-' || '17'
              FROM scmdata.RES_RTE_ASN_SRC routeres,
                   res_nonate_attr_src     resattr
             WHERE routeres.ALT_RTE_FLG = 'Y'
               AND routeres.enterprise = i_enterprise_nm
               AND routeres.prod_rte_seq_id = resattr.prod_rte_seq_id
               AND routeres.prim_res_set_id = resattr.prim_res_set_id
               and routeres.res_area = 'NON ATE FT'
               AND routeres.eng_rte_id = lv_routingid_na
               AND resattr.res_set_id =
                   curval_altrte_attr_na.alt_rte_res_set_id
            UNION
            SELECT distinct routeres.ENTERPRISE,
                            routeres.ENG_RTE_ID,
                            routeres.PROD_RTE_SEQ_ID,
                            routeres.PRIM_RES_SET_ID,
                            routeres.RTE_SEQ_NUM,
                            routeres.ENG_OPER_SEQ,
                            routeres.res_area,
                            routeres.ALT_RTE_FLG,
                            routeres.ALT_RES_FLG,
                            routeres.PRIM_RES_SET_ID alt_rte_res_set_id,
                            sysdate,
                            routeres.ETL_DT,
                            lv_row_source || '-' || '17'
              from scmdata.RES_RTE_ASN_SRC routeres
             WHERE routeres.ALT_RTE_FLG = 'Y'
               and routeres.enterprise = i_enterprise_nm
               and routeres.res_area = 'NON ATE FT'
               and routeres.eng_rte_id = lv_routingid_na
               and routeres.eng_oper_seq not in
                   (select distinct routeres.ENG_OPER_SEQ
                      from scmdata.RES_RTE_ASN_SRC routeres,
                           res_nonate_attr_src     resattr
                     WHERE routeres.ALT_RTE_FLG = 'Y'
                       and routeres.enterprise = i_enterprise_nm
                       and routeres.prod_rte_seq_id =
                           resattr.prod_rte_seq_id(+)
                       and routeres.prim_res_set_id =
                           resattr.prim_res_set_id(+)
                       and routeres.eng_rte_id = lv_routingid_na
                       and resattr.res_set_id =
                           curval_altrte_attr_na.alt_rte_res_set_id);
        
          commit;
        
        END IF;
      
      END LOOP;
    
    END LOOP;
  
    commit;
    frmdata.logs.info('RES_RTE_ASN_DTL_SRC load from SNP to SRC is completed');
  
  END LOAD_ROUTE_RES_ALT_DTLRTE_SRC;

  --************************************************************
  --Procedure to load data into ROUTE_ALT_SRC meant for alternate routes
  -- alternate routes for rows with alt_rte_flg = 'Y'
  --************************************************************
  PROCEDURE load_RES_RTE_ALT_SRC(i_enterprise_nm IN VARCHAR2) IS
  
    --  lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date
    ---below declarations for  ATE------------------
    lv_orig_routingid RES_RTE_ALT_SRC.ENG_ORIG_RTE_ID%TYPE;
    lv_operseq        RES_RTE_ALT_SRC.ENG_OPER_SEQ%TYPE;
    rec_cnt           INTEGER;
  
    CURSOR cur_res_dtl_src IS
      select distinct ENTERPRISE,
                      ENG_RTE_ID ORIG_ROUTINGID,
                      PROD_RTE_SEQ_ID,
                      PRIM_RES_SET_ID,
                      RTE_SEQ_NUM,
                      ENG_OPER_SEQ,
                      ALT_RTE_FLG,
                      ALT_RES_FLG,
                      res_area,
                      ETL_DT
      --dtl.*--,
      --    dtl.eng_routingid,
        from RES_RTE_ASN_DTL_SRC dtl
       where alt_rte_flg = 'Y'
            --   and eng_routingid = 'HS45Z_S1_2_4905_1'
         and enterprise = i_enterprise_nm
         and dtl.res_area <> 'NON ATE FT';
  
    -- CURSOR to get resource_set_ids for a given prim_resource_set_id from the res_attr_src table
  
    CURSOR cur_alt_rte_src IS
      select distinct alt_rte_res_set_id
        from RES_RTE_ASN_DTL_SRC dtl
       where alt_rte_flg = 'Y'
         and eng_rte_id = lv_orig_routingid
         and eng_oper_seq = lv_operseq
         and enterprise = i_enterprise_nm ----Added by VS 01/11/18
         and dtl.res_area <> 'NON ATE FT'
       order by alt_rte_res_set_id;
  
    curval_res_dtl cur_res_dtl_src%ROWTYPE;
    curval_alt_rte cur_alt_rte_src%ROWTYPE;
  
    -----------below data is for NON ATE------------
    lv_orig_routingid_na RES_RTE_ALT_SRC.ENG_ORIG_RTE_ID%TYPE;
    lv_operseq_na        RES_RTE_ALT_SRC.ENG_OPER_SEQ%TYPE;
    rec_cnt_na           INTEGER;
  
    CURSOR cur_res_dtl_src_na IS
      select distinct ENTERPRISE,
                      ENG_RTE_ID ORIG_ROUTINGID,
                      PROD_RTE_SEQ_ID,
                      PRIM_RES_SET_ID,
                      RTE_SEQ_NUM,
                      ENG_OPER_SEQ,
                      ALT_RTE_FLG,
                      ALT_RES_FLG,
                      RES_AREA,
                      ETL_DT
        from RES_RTE_ASN_DTL_SRC dtl
       where alt_rte_flg = 'Y'
         and res_area = 'NON ATE FT'
         and enterprise = i_enterprise_nm;
  
    CURSOR cur_alt_rte_src_na IS
      select distinct alt_rte_res_set_id
        from RES_RTE_ASN_DTL_SRC dtl
       where alt_rte_flg = 'Y'
         and eng_rte_id = lv_orig_routingid_na
         and eng_oper_seq = lv_operseq_na
         and enterprise = i_enterprise_nm ----Added by VS 01/11/18
         and res_area = 'NON ATE FT'
       order by alt_rte_res_set_id;
  
    curval_res_dtl_na cur_res_dtl_src_na%ROWTYPE;
    curval_alt_rte_na cur_alt_rte_src_na%ROWTYPE;
  
  BEGIN
  
    frmdata.logs.begin_log('Start RES_RTE_ALT_SRC load from SRC to ODS');
    frmdata.delete_data('scmdata.RES_RTE_ALT_SRC',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    -- lv_cur_date   := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
    lv_row_source := 'X001';
  
    ---below logic is for ATE-----------
  
    FOR curval_res_dtl IN cur_res_dtl_src LOOP
    
      lv_orig_routingid := curval_res_dtl.ORIG_ROUTINGID;
      lv_operseq        := curval_res_dtl.ENG_OPER_SEQ;
    
      rec_cnt := 0;
    
      FOR curval_alt_rte IN cur_alt_rte_src LOOP
      
        rec_cnt := rec_cnt + 1;
      
        INSERT INTO RES_RTE_ALT_SRC
          (ENTERPRISE,
           ENG_ORIG_RTE_ID,
           ENG_ALT_RTE_ID,
           ENG_OPER_SEQ,
           RES_SET_ID,
           res_area,
           ROW_SOURCE,
           SOURCE_DT,
           ETL_DT)
          SELECT curval_res_dtl.enterprise,
                 curval_res_dtl.orig_routingid,
                 curval_res_dtl.orig_routingid || '_ALT_' || rec_cnt,
                 curval_res_dtl.ENG_OPER_SEQ,
                 curval_alt_rte.alt_rte_res_set_id,
                 curval_res_dtl.res_area,
                 lv_row_source,
                 sysdate,
                 curval_res_dtl.ETL_DT
            FROM DUAL;
      END LOOP;
    
    END LOOP;
  
    -------below logic is for NON ATE---------
    FOR curval_res_dtl_na IN cur_res_dtl_src_na LOOP
    
      lv_orig_routingid_na := curval_res_dtl_na.ORIG_ROUTINGID;
      lv_operseq_na        := curval_res_dtl_na.ENG_OPER_SEQ;
    
      rec_cnt_na := 0;
    
      FOR curval_alt_rte_na IN cur_alt_rte_src_na LOOP
      
        rec_cnt_na := rec_cnt_na + 1;
      
        INSERT INTO RES_RTE_ALT_SRC
          (ENTERPRISE,
           ENG_ORIG_RTE_ID,
           ENG_ALT_RTE_ID,
           ENG_OPER_SEQ,
           RES_SET_ID,
           RES_AREA,
           ROW_SOURCE,
           SOURCE_DT,
           ETL_DT)
          SELECT curval_res_dtl_na.enterprise,
                 curval_res_dtl_na.orig_routingid,
                 curval_res_dtl_na.orig_routingid || '_ALT_' || rec_cnt_na,
                 curval_res_dtl_na.ENG_OPER_SEQ,
                 curval_alt_rte_na.alt_rte_res_set_id,
                 curval_res_dtl_na.RES_AREA,
                 lv_row_source,
                 sysdate,
                 curval_res_dtl_na.ETL_DT
            FROM DUAL;
      END LOOP;
    
    END LOOP;
  
    Commit; /* remove later  */
    frmdata.logs.info('RES_RTE_ALT_SRC load from SNP to SRC is completed');
  
  END load_RES_RTE_ALT_SRC;

  --------------------------------------------------------------------------------
  -- ALL these procs are used to load ODS tables for ALT_RTE flg= 'Y'
  ---------------------------------------------------------------------------------
  --************************************************************
  --Procedure to load data into ROUTINGHEADER ODS table
  --************************************************************

  PROCEDURE load_ROUTINGHEADER(i_enterprise_nm IN VARCHAR2) IS
  
  BEGIN
  
    frmdata.logs.begin_log('Start ROUTINGHEADER_STG load from SRC to ODS');
    -- frmdata.delete_data('scmdata.ROUTINGHEADER_STG', NULL, NULL);
  
    -- lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
  
    INSERT INTO ROUTINGHEADER_STG
      (ENTERPRISE,
       SITEID,
       ROUTINGID,
       ENGINE_ID,
       EFFSTARTDATE,
       EFFENDDATE,
       sourcedate)
      SELECT --alt.*,stg.*,    select --alt.*,dtl.*,head.*  ,
       eer.enterprise,
       stg.siteid,
       alt.ENG_ALT_RTE_ID,
       eer.engine_id,
       stg.effstartdate,
       stg.effenddate,
       sysdate
        FROM (select distinct ENG_ORIG_RTE_ID, ENG_ALT_RTE_ID, enterprise
                from RES_RTE_ALT_SRC
              /*where res_area <> 'NON ATE FT'*/
              ) alt,
             ROUTINGHEADER_STG stg,
             ref_data.eng_enterprise_ref eer
       WHERE alt.ENG_ORIG_RTE_ID = stg.routingid
         AND eer.enterprise = stg.enterprise
         AND CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = alt.enterprise
         AND eer.enterprise = i_enterprise_nm;
  
    Commit; /* remove later  */
    frmdata.logs.info('ROUTINGHEADER_STG load from SRC to ODS is completed');
  
  END load_ROUTINGHEADER;

  --************************************************************
  --Procedure to load data into ROUTINGOPERATION ODS table
  --************************************************************

  PROCEDURE load_ROUTINGOPERATION(i_enterprise_nm IN VARCHAR2) IS
  
  BEGIN
  
    frmdata.logs.begin_log('Start ROUTINGOPERATION_STG load from SRC to ODS');
    --   frmdata.delete_data('scmdata.ROUTINGOPERATION_STG', NULL, NULL);
  
    -- lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
  
    INSERT INTO RoutingOperation_STG
      (ENTERPRISE,
       SITEID,
       ROUTINGID,
       OPERATION,
       OPERATIONSEQ,
       OPTYPE,
       AVGFIXEDRUNTIME,
       TIMEUOM,
       ENGINE_ID,
       VARQTYREJECTED,
       sourcedate)
      select eer.enterprise,
             -- ENG_ORIG_RTE_ID,
             stg.siteid,
             alt.ENG_ALT_RTE_ID,
             stg.operation,
             stg.operationseq,
             'Manufacturing' as optype,
             stg.AVGFIXEDRUNTIME,
             'HOUR' as TIMEUOM,
             eer.engine_id,
             stg.VARQTYREJECTED,
             SYSDATE
        from routingoperation_stg stg,
             (select distinct ENG_ORIG_RTE_ID,
                              ENG_ALT_RTE_ID,
                              ENG_OPER_SEQ,
                              enterprise
                from RES_RTE_ALT_SRC
              /*where res_area <> 'NON ATE FT'*/
              ) alt,
             ref_data.eng_enterprise_ref eer
       WHERE alt.ENG_ORIG_RTE_ID = stg.routingid
         AND alt.ENG_OPER_SEQ = stg.operationseq
            -- and     alt.ENG_ORIG_RTE_ID IN ( 'MAX3799ETJ+_T1_S_4900_3','MAX3799ETJ+_T1_S_4900_1','MAX3799ETJ+_T1_S_4900_2')
         AND eer.enterprise = stg.enterprise --chanded to eer by SA on 1/10
         AND CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = alt.enterprise
         AND eer.enterprise = i_enterprise_nm;
  
    /* select eer.enterprise,
          -- ENG_ORIG_RTE_ID,
          head.eng_facility,
          alt.ENG_ALT_RTE_ID,
          head.matl_type_cd || '_' || head.ENG_OPER_SEQ OPERATION,
          head.ENG_OPER_SEQ,
          'MANUFACTURING' as optype,
          (select avgfixedruntime
              from routingoperation_stg
             where routingid = alt.ENG_ORIG_RTE_ID -- 'eng_routingid =
                 -- 'HS45Z_S1_2_4905_1'
              and operationseq = alt.ENG_OPER_SEQ) AVGFIXEDRUNTIME,
          'HOUR' as TIMEUOM,
          eer.engine_id,
          (select varqtyrejected
              from routingoperation_stg
             where routingid = alt.ENG_ORIG_RTE_ID -- 'eng_routingid =
                 --  'HS45Z_S1_2_4905_1'
              and operationseq = alt.ENG_OPER_SEQ) VARQTYREJECTED,
          SYSDATE
     FROM (select distinct ENG_ORIG_RTE_ID,
                           ENG_ALT_RTE_ID,
                           ENG_OPER_SEQ
             from RES_RTE_ALT_SRC) alt,
          --     RES_RTE_ASN_DTL_SRC dtl ,
          (select distinct enterprise,
                           eng_routingid,
                           eng_facility,
                           ENG_OPER_SEQ,
                           matl_type_cd,
                           eff_start_dt,
                           eff_end_dt
             from RES_RTE_ASN_SRC_NEW) head,
          ref_data.eng_enterprise_ref eer
    where alt.ENG_ORIG_RTE_ID = head.eng_routingid
      AND alt.ENG_OPER_SEQ = head.ENG_OPER_SEQ
      AND eer.enterprise = head.enterprise
      AND eer.enterprise = i_enterprise_nm; */
  
    Commit; /* remove later  */
    frmdata.logs.info('ROUTINGOPERATION_STG load from SRC to ODS is completed');
  
  END load_ROUTINGOPERATION;

  --************************************************************
  --Procedure to load data into ROUTINGOPERATION ODS table
  --************************************************************

  PROCEDURE load_ITEMBOMROUTING(i_enterprise_nm IN VARCHAR2) IS
  
  BEGIN
  
    frmdata.logs.begin_log('Start ITEMBOMROUTING load from SRC to ODS');
    --  frmdata.delete_data('scmdata.ITEMBOMROUTING_STG', NULL, NULL);
  
    -- lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
  
    INSERT INTO ITEMBOMROUTING_STG
      (ENTERPRISE,
       SITEID,
       ITEM,
       BOMID,
       ROUTINGID,
       ENGINE_ID,
       PRIORITY,
       sourcedate)
    -- 211 rows
      SELECT eer.enterprise,
             stg.SITEID,
             stg.ITEM,
             stg.BOMID,
             alt.ENG_ALT_RTE_ID,
             eer.engine_id,
             1 PRIORITY,
             SYSDATE
        FROM -- ROUTE_ALT_SRC alt,
             (select distinct ENG_ORIG_RTE_ID, ENG_ALT_RTE_ID, enterprise
                from RES_RTE_ALT_SRC
              /* where res_area <> 'NON ATE FT'*/
              ) alt,
             ITEMBOMROUTING_STG stg,
             ref_data.eng_enterprise_ref eer
       WHERE alt.ENG_ORIG_RTE_ID = stg.routingid
         AND eer.enterprise = stg.enterprise
         AND CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = alt.enterprise
         AND eer.enterprise = i_enterprise_nm;
    /*  SELECT eer.enterprise,
          (select siteid
             from itembomrouting_stg
            where routingid = alt.ENG_ORIG_RTE_ID) SITEID,
          (select item
             from itembomrouting_stg
            where routingid = alt.ENG_ORIG_RTE_ID) ITEM,
          (select bomid
             from itembomrouting_stg
            where routingid = alt.ENG_ORIG_RTE_ID) BOMID,
          alt.ENG_ALT_RTE_ID,
          eer.engine_id,
          1 PRIORITY,
          SYSDATE
     FROM -- RES_RTE_ALT_SRC alt,
          (select distinct ENG_ORIG_RTE_ID, ENG_ALT_RTE_ID
             from RES_RTE_ALT_SRC) alt,
          --     RES_RTE_ASN_DTL_SRC dtl ,
          (select distinct enterprise,
                           eng_routingid,
                           eng_facility,
                           eff_start_dt,
                           eff_end_dt
             from RES_RTE_ASN_SRC_NEW) head,
          ref_data.eng_enterprise_ref eer
    WHERE alt.ENG_ORIG_RTE_ID = head.eng_routingid
      AND eer.enterprise = head.enterprise
      AND eer.enterprise = i_enterprise_nm;*/
  
    Commit; /* remove later  */
    frmdata.logs.info('ITEMBOMROUTING_STG load from SRC to ODS is completed');
  
  END load_ITEMBOMROUTING;

  --************************************************************
  --Procedure to load data into OPCALENDAR ODS table
  --************************************************************

  PROCEDURE LOAD_OPCALENDAR_STG(i_enterprise_nm IN VARCHAR2) IS
  
  BEGIN
  
    frmdata.logs.begin_log('StartOPCALENDAR_STG load from SRC to ODS');
    --  frmdata.delete_data('scmdata.OPCALENDAR_STG', NULL, NULL);
  
    INSERT INTO OPCALENDAR_STG
      (CALENDARNAME,
       CALENDARTYPE,
       ENGINE_ID,
       ENTERPRISE,
       OPERATION,
       --RESOURCENAME,
       --RESOURCESITEID,
       ROUTINGID,
       SITEID,
       --WORKCENTERNAME,
       sourcedate)
      select cal.CALENDARNAME,
             'YIELD' AS CALENDARTYPE,
             eer.engine_id AS ENGINE_ID,
             eer.enterprise AS ENTERPRISE,
             cal.OPERATION,
             alt.ENG_ALT_RTE_ID,
             cal.siteid,
             sysdate
        from opcalendar_stg cal,
             (select distinct ENG_ORIG_RTE_ID, ENG_ALT_RTE_ID, enterprise
                from RES_RTE_ALT_SRC
              /* where res_area <> 'NON ATE FT'*/
              ) alt,
             ref_data.eng_enterprise_ref eer
       WHERE alt.ENG_ORIG_RTE_ID = cal.routingid
            --and     alt.ENG_ORIG_RTE_ID IN ( 'MAX3799ETJ+_T1_S_4900_3','MAX3799ETJ+_T1_S_4900_1','MAX3799ETJ+_T1_S_4900_2')
         AND eer.enterprise = cal.enterprise
         AND CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = alt.enterprise
         AND eer.enterprise = i_enterprise_nm;
  
    /*Below logic is for shutdown calendar*/ ----Added by VS 01/04/2018
    INSERT INTO OPCALENDAR_STG
      select distinct 'HOL' || '_' || smr.eng_site_num || '_' ||
                      sdc.area_cd calendarname,
                      'WORKING' calendartype,
                      eer.engine_id,
                      eer.enterprise,
                      ro.operation,
                      null resourcename,
                      null resourcesiteid,
                      ro.routingid,
                      ro.siteid,
                      null workcentername,
                      sysdate sourcedate
        from cal_shut_down_ref_snp_vw  sdc, ------------added vw
             ref_data.site_master_ref  smr,
             rte_src                   rs,
             routingoperation_stg      ro,
             eng_enterprise_ref_snp_vw eer
       where sdc.site_num = smr.site_num
         and smr.eng_site_num = rs.eng_site_num
         and rs.area_cd = sdc.area_cd
         and eer.enterprise = i_enterprise_nm
         and eer.enterprise = ro.enterprise
         and rs.eng_site_num = ro.siteid
         and ro.routingid like '%' || rs.eng_rte_id || '%';
  
    commit;
  
  END LOAD_OPCALENDAR_STG;

  --************************************************************
  --  -- This procedure is used to load data for Sort and Test resources and Fab resources and Assembly/Bump/DieCoat resources
  -- Sort and Test details below
  -- Procedure to load data into OPRESOURCE table --7 -- ALT_RES_FLG= 'Y'
  -- and ALT_RES_FLG = 'N' and ALT_RTE_FLG = 'N'
  -- and ALT_RTE_FLG = Y'
  -- This procedure adds TESTER in OPRESOURCE table for ALL primary resource set ids for
  --************************************************************

  PROCEDURE load_OPRESOURCE(i_enterprise_nm IN VARCHAR2) IS
  
    -- lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date
  
  BEGIN
  
    frmdata.logs.begin_log('Start OPRESOURCE load from SRC to ODS');
    --frmdata.delete_data('scmdata.OPRESOURCE_STG', NULL, NULL);
    frmdata.delete_data('OPRESOURCE_STG',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    --   lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
  
    -- IF i_enterprise_nm IN ('SCP_STARTS','OP_DAILY') THEN
    INSERT INTO opresource_stg
      (enterprise,
       routingid,
       operationseq,
       siteid,
       workcentername,
       resourcename,
       runtimeper,
       engine_id,
       sourcedate,
       resourcesiteid,
       operation,
       pri,
       prodrate)
    /*         SELECT DISTINCT * FROM
            (
    */
      SELECT DISTINCT eer.enterprise,
                      ros.routingid,
                      ros.operationseq,
                      ros.siteid,
                      rfa.prim_res_nm || '_' || ros.siteid workcentername,
                      rfa.prim_res_nm || '_' || ros.siteid resourcename,
                      rfa.wip_uph,
                      eer.engine_id,
                      sysdate,
                      'RES_SITE' res_site,
                      ros.operation,
                      null pri,
                      null prodrate
        FROM itembomrouting_stg ibr,
             res_fab_asn_src    rfa,
             RESOURCEMASTER_STG RCS,
             -- RES_CAP_SRC                 RCS,
             routingoperation_stg        ros,
             ref_data.eng_enterprise_ref eer
       WHERE eer.enterprise = i_enterprise_nm
         AND eer.enterprise IN ('SCP_STARTS', 'OP_DAILY') --ADDED BY SA on 12/20
         AND CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_STARTS'
               ELSE
                eer.enterprise --ADDED BY SA on 12/20
             END = rfa.enterprise
         AND RCS.ENTERPRISE = eer.ENTERPRISE
         AND RCS.RESOURCENAME = RFA.RES_NM || '_' || RFA.ENG_SITE_NUM
         AND eer.enterprise = ibr.enterprise
         AND rfa.mfg_part_num = ibr.item
         and rfa.eng_site_num = ibr.siteid
         AND ibr.routingid = ros.routingid
         AND rfa.eng_oper_seq = ros.operationseq
         AND ros.enterprise = eer.enterprise
         AND rfa.alt_type = 'PRIM';
    /*         )
             WHERE WORKCENTERNAME IN (SELECT DISTINCT resourcename
                                        FROM resourcemaster_stg
                                       WHERE enterprise LIKE 'SCP_STARTS');
    */
    -- ELSE
    -----------ATE-------------------
    /***** Inserting data for alt_res_flg = 'N' and alt_rte_flg = 'N'  Sort and test resources
    add resources TESTER  'SCP_DAILY Engine ********/
    INSERT INTO OPRESOURCE_STG
      (ENTERPRISE,
       ROUTINGID,
       OPERATIONSEQ,
       SITEID,
       WORKCENTERNAME,
       RESOURCENAME,
       ENGINE_ID,
       sourcedate,
       RESOURCESITEID,
       OPERATION,
       PRODRATE)
      SELECT *
        FROM (SELECT eer.enterprise,
                     ro.routingid,
                     ro.operationseq,
                     ro.siteid,
                     rc.eng_res_nm   WORKCENTERNAME,
                     rc.eng_res_nm   RESOURCENAME,
                     --   ra.resource_nm || '_' || ldr.list_val_char || ro.siteid WORKCENTERNAME,
                     --   ra.resource_nm || '_' || ldr.list_val_char || ro.siteid RESOURCENAME,
                     eer.engine_id,
                     sysdate as sourcedate,
                     'RES_SITE' as RESOURCESITEID,
                     ro.operation,
                     ra.uph as PRODRATE
                FROM --routing_res_alt_src rc,
                     (select distinct enterprise,
                                      eng_rte_id,
                                      eng_oper_seq,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area
                        from RES_RTE_ASN_DTL_SRC dtl
                       where alt_res_flg = 'N'
                         and alt_rte_flg = 'N') dtl,
                     routingoperation_stg ro,
                     scmdata.res_attr_src ra,
                     (select distinct eng_rte_id,
                                      enterprise, --added byt SA on 1/8
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area
                        from RES_PART_ASN_SRC) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      enterprise, --added byt SA on 1/8
                                      res_area
                        from RES_CAP_SRC) rc
               WHERE ro.routingid = dtl.eng_rte_id
                 AND ro.operationseq = dtl.eng_oper_seq
                 AND ra.res_nm = rc.res_nm
                 AND rc.res_area = pra.res_area
                 and rc.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                    /*and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                    and pra.eng_rte_id not like 'MAX1480BEPI+%'*/
                 and dtl.res_area = pra.res_area
                 AND ro.siteid = rc.site_num
                 AND ra.res_type = 'TESTER'
                 AND ra.res_nm is NOT NULL
                 AND ra.res_nm not like 'BLIN%'
                 AND ra.prim_res_set_id = ra.res_set_id
                 AND ra.prim_res_set_id = pra.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND ra.prim_res_set_id = dtl.prim_res_set_id
                 AND eer.enterprise = ro.enterprise
                 AND rc.enterprise = dtl.enterprise --added by SA on 1/8
                 AND rc.enterprise = pra.enterprise --added by SA on 1/8
                 AND eer.enterprise <> 'SCP_STARTS' --ADDED BY SA on 12/20
                 AND CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND eer.enterprise = i_enterprise_nm);
  
    /***** Inserting data for alt_res_flg = 'Y' and alt_rte_flg = 'N'  Sort and test resources ********/
  
    INSERT INTO OPRESOURCE_STG
      (ENTERPRISE,
       ROUTINGID,
       OPERATIONSEQ,
       SITEID,
       WORKCENTERNAME,
       RESOURCENAME,
       ENGINE_ID,
       sourcedate,
       RESOURCESITEID,
       OPERATION,
       PRODRATE)
      SELECT *
        FROM (SELECT eer.enterprise,
                     ro.routingid,
                     ro.operationseq,
                     ro.siteid,
                     rc.eng_res_nm WORKCENTERNAME,
                     rc.eng_res_nm RESOURCENAME,
                     eer.engine_id,
                     sysdate as sourcedate,
                     'RES_SITE' as RESOURCESITEID,
                     ro.operation,
                     ra.uph as PRODRATE
                FROM --routing_res_alt_src rc,
                     (select distinct enterprise,
                                      eng_rte_id,
                                      eng_oper_seq,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area
                        from RES_RTE_ASN_DTL_SRC dtl
                       where alt_rte_flg = 'N'
                         and alt_res_flg = 'Y') dtl,
                     routingoperation_stg ro,
                     scmdata.res_attr_src ra,
                     (select distinct eng_rte_id,
                                      enterprise, --added by SA on 1/8
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area
                        from RES_PART_ASN_SRC) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      res_area,
                                      enterprise --added by SA on 1/8
                        from RES_CAP_SRC) rc
               WHERE ro.routingid = dtl.eng_rte_id
                 AND ro.operationseq = dtl.eng_oper_seq
                 AND ra.res_nm = rc.res_nm
                 AND rc.res_area = pra.res_area
                 and rc.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 AND ro.siteid = rc.site_num
                 AND ra.res_type = 'TESTER'
                 AND ra.res_nm is NOT NULL
                 AND ra.res_nm not like 'BLIN%'
                 AND ra.prim_res_set_id = ra.res_set_id
                    /*and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                    and pra.eng_rte_id not like 'MAX1480BEPI+%'*/
                 AND ra.prim_res_set_id = pra.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND ra.prim_res_set_id = dtl.prim_res_set_id
                 AND eer.enterprise = ro.enterprise
                 AND rc.enterprise = dtl.enterprise --added by SA on 1/8
                 AND rc.enterprise = pra.enterprise --added by SA on 1/8
                    
                 AND eer.enterprise <> 'SCP_STARTS' --ADDED BY SA on 12/20
                 AND CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND eer.enterprise = i_enterprise_nm);
  
    --  for those alt routes
    -- where  ALT_RTE_FLG= 'Y' sort and test resources
  
    INSERT INTO OPRESOURCE_STG
      (ENTERPRISE,
       ROUTINGID,
       OPERATIONSEQ,
       SITEID,
       WORKCENTERNAME,
       RESOURCENAME,
       ENGINE_ID,
       sourcedate,
       RESOURCESITEID,
       OPERATION,
       PRODRATE)
      SELECT *
        FROM (SELECT eer.enterprise,
                      oper.routingid,
                      oper.operationseq,
                      oper.siteid,
                      rc.eng_res_nm     WORKCENTERNAME,
                      rc.eng_res_nm     RESOURCENAME,
                      eer.engine_id,
                      sysdate,
                      'RES_SITE',
                      oper.operation,
                      attr.uph
                 FROM RoutingOperation_STG oper,
                      RES_RTE_ALT_SRC alt,
                      RES_RTE_ASN_DTL_SRC dtl,
                      RES_ATTR_SRC attr,
                      (select distinct eng_rte_id,
                                       enterprise, --added by SA on 1/8
                                       prod_rte_seq_id,
                                       prim_res_set_id,
                                       res_area
                         from RES_PART_ASN_SRC) pra,
                      ref_data.list_dtl_ref ldr,
                      ref_data.eng_enterprise_ref eer,
                      (select distinct res_nm,
                                       site_num,
                                       eng_res_nm,
                                       res_area,
                                       enterprise --added by SA on 1/8
                         from RES_CAP_SRC) rc
                WHERE -- oper.routingid =  'HS45Z_S1_2_4905_1_ALT_7' and
                oper.routingid = alt.ENG_ALT_RTE_ID
               --and   alt.ENG_ORIG_RTE_ID   = 'HS45Z_S1_2_4905_1'
             AND oper.operationseq = alt.ENG_OPER_SEQ
             AND alt.ENG_ORIG_RTE_ID = dtl.eng_rte_id
             AND alt.res_set_id = dtl.alt_rte_res_set_id
             AND alt.ENG_OPER_SEQ = dtl.eng_oper_seq
             AND dtl.prod_rte_seq_id = attr.prod_rte_seq_id
               /* and oper.routingid not like 'MAX1480AEPI+_S_4900%'
               and pra.eng_rte_id not like 'MAX1480BEPI+%'*/
             AND dtl.prim_res_set_id = attr.prim_res_set_id
             AND dtl.alt_rte_res_set_id = attr.res_set_id
             AND pra.res_area = ldr.list_dtl_num
             AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
             AND pra.prim_res_set_id = dtl.prim_res_set_id
             AND attr.res_type = 'TESTER'
             AND attr.res_nm not like 'BLIN%'
             AND attr.res_nm = rc.res_nm
             AND rc.res_area = pra.res_area
             and rc.res_area not in ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
             and rc.res_area = pra.res_area
             and pra.res_area = dtl.res_area
             and alt.res_area = dtl.res_area
             AND oper.siteid = rc.site_num
             AND oper.enterprise = eer.enterprise
             AND alt.enterprise = dtl.enterprise
             AND rc.enterprise = dtl.enterprise --added by SA on 1/8
             AND rc.enterprise = pra.enterprise --added by SA on 1/8
               
             AND CASE
                  WHEN i_enterprise_nm LIKE 'OP%' THEN
                   'SCP_DAILY'
                  ELSE
                   eer.enterprise
                END = dtl.enterprise
             AND eer.enterprise <> 'SCP_STARTS' --ADDED BY SA on 12/20
               
             AND eer.enterprise = i_enterprise_nm);
  
    /***** Inserting data for SCP_DAILY Assembly/Bump/Diecoat resources ********/
  
    INSERT INTO OPRESOURCE_STG
      (ENTERPRISE,
       ROUTINGID,
       OPERATIONSEQ,
       SITEID,
       WORKCENTERNAME,
       RESOURCENAME,
       ENGINE_ID,
       sourcedate,
       RESOURCESITEID,
       OPERATION,
       RUNTIMEPER)
      SELECT DISTINCT eer.enterprise,
                      ro.routingid,
                      ro.operationseq,
                      ro.siteid, --apv.eng_site_num,
                      rc.eng_res_nm WORKCENTERNAME,
                      rc.eng_res_nm RESOURCENAME,
                      eer.engine_id,
                      sysdate as sourcedate,
                      'RES_SITE' as RESOURCESITEID,
                      -- apv.run_time_per
                      ro.operation,
                      apv.run_time_per
        FROM ref_data.eng_enterprise_ref eer,
             (select distinct res_nm,
                              site_num,
                              eng_res_nm,
                              res_area,
                              enterprise --added by SA on 1/8
                from RES_CAP_SRC
               where res_area in ('DIECOAT', 'BUMP', 'PKG')) rc,
             (select enterprise,
                      routingid,
                      operationseq,
                      operation,
                      siteid,
                      substr(routingid,
                              1,
                              (instr(routingid, siteid, 1, 1) - 1 + -- Noted that Routingid includes the timephasing logic with the last ?_% this needs to be excluded as assembly resources don't have time phasing
                             length(siteid))) as routingidntp
                from routingoperation_stg
               where -- routingid = 'MAX15046B_EE+_A1_1_ASSY_GNRC_1'  and
               enterprise = i_enterprise_nm --added by SA on 1/11
              ) ro,
             (select distinct eng_site_num,
                              --    vnd_num,
                              res_nm,
                              res_cd,
                              eng_rte_id,
                              eng_oper_seq,
                              run_time_per,
                              enterprise
                FROM Assy_Part_Vnd_Src
               where eng_rte_id is not null -- 861
              ) apv
       WHERE ro.routingidntp = apv.eng_rte_id
         AND ro.operationseq = apv.eng_oper_seq
         and ro.siteid = apv.eng_site_num
         AND apv.res_nm = rc.res_nm
            -- AND    rc.res_area = pra.res_area
         AND apv.eng_site_num = rc.site_num
         AND eer.enterprise = ro.enterprise
         AND rc.enterprise = apv.enterprise
         AND CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = apv.enterprise
         AND eer.enterprise = i_enterprise_nm
         AND eer.enterprise <> 'SCP_STARTS' --ADDED BY SA on 12/20
      ;
  
    -----Populates data for NON ATE Resources.
  
    /*Inserting data  NON ATE BIOV  **/
  
    INSERT INTO Opresource_Stg
      (ENTERPRISE,
       ROUTINGID,
       OPERATIONSEQ,
       SITEID,
       WORKCENTERNAME,
       RESOURCENAME,
       ENGINE_ID,
       sourcedate,
       RESOURCESITEID,
       OPERATION,
       PRODRATE)
      SELECT eer.enterprise,
             ro.routingid,
             ro.operationseq,
             ro.siteid,
             rc.eng_res_nm WORKCENTERNAME,
             rc.eng_res_nm RESOURCENAME,
             eer.engine_id,
             sysdate as sourcedate,
             'RES_SITE' as RESOURCESITEID,
             ro.operation,
             null as PRODRATE
        FROM (select distinct enterprise,
                              eng_rte_id,
                              eng_oper_seq,
                              prod_rte_seq_id,
                              prim_res_set_id
                from RES_RTE_ASN_DTL_SRC dtl
               where alt_res_flg = 'N'
                 and alt_rte_flg = 'N'
                 and res_area = 'NON ATE FT') dtl,
             routingoperation_stg ro,
             scmdata.res_nonate_attr_src ra,
             (select distinct eng_rte_id,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area,
                              enterprise
                from RES_PART_ASN_SRC
               where res_area = 'NON ATE FT') pra,
             ref_data.list_dtl_ref ldr,
             ref_data.eng_enterprise_ref eer,
             (select distinct res_nm,
                              site_num,
                              eng_res_nm,
                              res_area,
                              enterprise
                from RES_CAP_SRC
               where res_area like 'NON%') rc
       WHERE CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = dtl.enterprise
         and eer.enterprise = ro.enterprise
         and dtl.enterprise = ra.enterprise
         and ra.enterprise = pra.enterprise
         and pra.enterprise = rc.enterprise
         AND eer.enterprise = i_enterprise_nm
         AND eer.enterprise <> 'SCP_STARTS' --ADDED BY SA on 12/20
            --and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
         and ro.routingid like '%' || dtl.eng_rte_id || '%'
         AND ro.operationseq = dtl.eng_oper_seq
         AND ra.res_nm = rc.res_nm
         AND rc.res_area like '%' || pra.res_area || '%'
         AND ro.siteid = rc.site_num
         AND ra.res_type = 'BIOV'
         AND ra.res_nm is NOT NULL
         AND ra.prim_res_set_id = ra.res_set_id
         AND ra.prim_res_set_id = pra.prim_res_set_id
         AND pra.res_area = ldr.list_dtl_num
         AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND pra.prim_res_set_id = dtl.prim_res_set_id
         AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND ra.prim_res_set_id = dtl.prim_res_set_id --MAX1480AEPI+_S_4900_1
      union all ---alt_res_flg = Y and alt_rte_flg = 'n'
      SELECT eer.enterprise,
             ro.routingid,
             ro.operationseq,
             ro.siteid,
             rc.eng_res_nm WORKCENTERNAME,
             rc.eng_res_nm RESOURCENAME,
             eer.engine_id,
             sysdate as sourcedate,
             'RES_SITE' as RESOURCESITEID,
             ro.operation,
             null as PRODRATE
        FROM (select distinct enterprise,
                              eng_rte_id,
                              eng_oper_seq,
                              prod_rte_seq_id,
                              prim_res_set_id
                from RES_RTE_ASN_DTL_SRC dtl
               where alt_rte_flg = 'N'
                 and alt_res_flg = 'Y'
                 and res_area like 'NON ATE FT') dtl,
             routingoperation_stg ro,
             scmdata.res_nonate_attr_src ra,
             (select distinct eng_rte_id,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area,
                              enterprise
                from RES_PART_ASN_SRC
               where res_area like 'NON ATE FT') pra,
             ref_data.list_dtl_ref ldr,
             ref_data.eng_enterprise_ref eer,
             (select distinct res_nm,
                              site_num,
                              eng_res_nm,
                              res_area,
                              enterprise
                from RES_CAP_SRC
               where res_area like 'NON%') rc
       WHERE CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = dtl.enterprise
         AND eer.enterprise <> 'SCP_STARTS' --ADDED BY SA on 12/20
         and dtl.enterprise = ra.enterprise
         and ro.enterprise = eer.enterprise
         and ra.enterprise = pra.enterprise
            --   and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
         and pra.enterprise = rc.enterprise
         AND eer.enterprise = i_enterprise_nm
         and ro.routingid = dtl.eng_rte_id
         AND ro.operationseq = dtl.eng_oper_seq
         AND ra.res_nm = rc.res_nm
         AND rc.res_area like '%' || pra.res_area || '%'
         AND ro.siteid = rc.site_num
         AND ra.res_type = 'BIOV'
         AND ra.res_nm is NOT NULL
         AND ra.prim_res_set_id = ra.res_set_id
         AND ra.prim_res_set_id = pra.prim_res_set_id
         AND pra.res_area = ldr.list_dtl_num
         AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND pra.prim_res_set_id = dtl.prim_res_set_id
         AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND ra.prim_res_set_id = dtl.prim_res_set_id --MAX17845ACB/V+CJT_T1_S_4900_1
      union all ----alt_rte_flg='Y'
      SELECT eer.enterprise,
             oper.routingid,
             oper.operationseq,
             oper.siteid,
             rc.eng_res_nm     WORKCENTERNAME,
             rc.eng_res_nm     RESOURCENAME,
             eer.engine_id,
             sysdate,
             'RES_SITE',
             oper.operation,
             null              uph
        FROM RoutingOperation_STG oper,
             RES_RTE_ALT_SRC alt,
             RES_RTE_ASN_DTL_SRC dtl,
             RES_nonate_ATTR_SRC attr,
             (select distinct eng_rte_id,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area,
                              enterprise
                from RES_PART_ASN_SRC
               where res_area like 'NON ATE FT') pra,
             ref_data.list_dtl_ref ldr,
             ref_data.eng_enterprise_ref eer,
             (select distinct res_nm,
                              site_num,
                              eng_res_nm,
                              res_area,
                              enterprise
                from RES_CAP_SRC
               where res_area like 'NON%') rc
       WHERE oper.enterprise = eer.enterprise
         AND alt.enterprise = dtl.enterprise
         AND CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = dtl.enterprise
         and dtl.enterprise = attr.enterprise
         and pra.enterprise = attr.enterprise
         and rc.enterprise = pra.enterprise
         AND eer.enterprise = i_enterprise_nm
         and alt.ENG_ALT_RTE_ID like '%' || oper.routingid || '%'
         AND oper.operationseq = alt.ENG_OPER_SEQ
            --  and oper.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
         AND alt.ENG_ORIG_RTE_ID = dtl.eng_rte_id
         AND alt.res_set_id = dtl.alt_rte_res_set_id
         AND alt.ENG_OPER_SEQ = dtl.eng_oper_seq
         AND dtl.prod_rte_seq_id = attr.prod_rte_seq_id
         AND dtl.prim_res_set_id = attr.prim_res_set_id
         AND dtl.alt_rte_res_set_id = attr.res_set_id
         AND pra.res_area = ldr.list_dtl_num
         AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND pra.prim_res_set_id = dtl.prim_res_set_id
         AND attr.res_type = 'BIOV'
         AND attr.res_nm = rc.res_nm
         AND rc.res_area like '%' || pra.res_area || '%'
         and alt.res_area = pra.res_area
         AND oper.siteid = rc.site_num
         and dtl.res_area = 'NON ATE FT'
         AND eer.enterprise <> 'SCP_STARTS' --ADDED BY SA on 12/20
      ;
  
    --END IF;
  
    INSERT INTO opresource_stg
      (enterprise,
       routingid,
       operationseq,
       siteid,
       workcentername,
       resourcename,
       engine_id,
       sourcedate,
       resourcesiteid,
       operation)
      SELECT DISTINCT enterprise,
                      routingid,
                      operationseq,
                      siteid,
                      'DUMMY',
                      'DUMMY',
                      engine_id,
                      SYSDATE,
                      enterprise,
                      operation
        FROM routingoperation_stg
       WHERE enterprise = i_enterprise_nm
         and enterprise like '%FP_MRP%';
  
    Commit; /* remove later  */
    frmdata.logs.info('OPRESOURCE_STG load from SRC to ODS is completed');
  
  END load_OPRESOURCE;

  --************************************************************
  --Procedure to load data into OPRESOURCEADD table --8 -- ALT_RES_FLG= 'Y'
  -- and ALT_RES_FLG = 'N' and ALT_RTE_FLG = 'N'
  -- and ALT_RTE_FLG = Y'
  -- This procedure adds HANDLERS and HW_SET_ID's  in OPRESOURCEADD table for ALL primary resource set ids
  --************************************************************
  PROCEDURE load_OPRESOURCEADD(i_enterprise_nm IN VARCHAR2) IS
  
    --  lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date
  
  BEGIN
  
    frmdata.logs.begin_log('Start OPRESOURCEADD load from SRC to ODS');
    --frmdata.delete_data('scmdata.OPRESOURCEADD_STG', NULL, NULL);
    frmdata.delete_data('OPRESOURCEADD_STG',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    --lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
  
    /***** Inserting data for alt_res_flg = 'N' and alt_rte_flg = 'N' ********/
  
    -- simultaneous resources HANDLERS
  
    --  IF i_enterprise_nm = 'SCP_STARTS' THEN
    INSERT INTO OPRESOURCEADD_STG
      (addresourcename,
       addresourcesiteid,
       addworkcentername,
       engine_id,
       enterprise,
       operation,
       operationseq,
       resourcename,
       routingid,
       runtimeper,
       siteid,
       workcentername,
       sourcedate)
    /*         SELECT DISTINCT * FROM
    (*/
      SELECT distinct rfa.res_nm || '_' || ibr.siteid addresourcename,
                      'RES_SITE' addresourcesiteid,
                      rfa.res_nm || '_' || ibr.siteid addworkcentername,
                      eer.engine_id,
                      eer.enterprise,
                      os.operation,
                      os.operationseq,
                      os.resourcename,
                      os.routingid,
                      rfa.wip_uph,
                      os.siteid siteid,
                      os.workcentername,
                      sysdate
        FROM ref_data.eng_enterprise_ref eer,
             opresource_stg              os,
             res_fab_asn_src             rfa,
             resourcemaster_stg          rs,
             itembomrouting_stg          ibr /*,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           routingoperation_stg        ros*/
       WHERE eer.enterprise IN ('SCP_STARTS', 'OP_DAILY') --ADDED BY SA on 12/20
            --  AND EER.ENTERPRISE = DUMMY.ENTERPRISE AND RFA.RES_AREA LIKE DUMMY.RES_AREA AND
         AND eer.enterprise = i_enterprise_nm
         AND CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_STARTS'
               ELSE
                eer.enterprise --ADDED BY SA on 12/20
             END = rfa.enterprise
         AND eer.enterprise = os.enterprise
         and os.enterprise = rs.enterprise
         AND ibr.enterprise = os.enterprise --added by SA on 1/9
         and rs.resourcename = rfa.res_nm || '_' || ibr.siteid --os.addworkcentername
            -- AND ros.routingid = ibr.routingid
         AND os.routingid = ibr.routingid
         AND ibr.item = rfa.mfg_part_num
         AND ibr.siteid = rfa.eng_site_num
         AND os.operationseq = rfa.eng_oper_seq
         AND substr(os.resourcename,
                    0,
                    instr(os.resourcename, '_', 1, 1) - 1) =
             rfa.prim_res_nm
         AND rfa.alt_type = 'SIMUL';
    /*       )
            WHERE addworkcentername IN (SELECT DISTINCT resourcename
                                          FROM resourcemaster_stg
                                         WHERE enterprise LIKE 'SCP_STARTS');
    */
    -- ELSE
    commit;
    -----ATE-------------
    DBMS_OUTPUT.PUT_LINE(1 || '-' || SYSTIMESTAMP || i_enterprise_nm);
    INSERT INTO OPRESOURCEADD_STG
      (ENGINE_ID,
       ENTERPRISE,
       OPERATION, -- ROUTINGHEADER>OPERATION
       OPERATIONSEQ,
       RESOURCENAME,
       ROUTINGID,
       SITEID,
       WORKCENTERNAME,
       sourcedate,
       ADDRESOURCENAME,
       ADDRESOURCESITEID, -- hardcode as 'RES_SITE'
       ADDWORKCENTERNAME,
       PRODRATE)
      SELECT *
        FROM (SELECT distinct eer.engine_id,
                              eer.enterprise,
                              ro.operation,
                              ro.operationseq,
                              odsopres.resourcename, -- resourcename
                              odsopres.routingid,
                              odsopres.siteid,
                              odsopres.workcentername,
                              sysdate,
                              --  pra.res_area,
                              --  ldr.list_val_char,
                              --   ra.res_nm || '_' || ldr.list_val_char ||
                              --   ro.siteid ADDRESOURCENAME,
                              rc.eng_res_nm ADDRESOURCENAME,
                              'RES_SITE' as ADDRESOURCESITEID,
                              --  ra.res_nm || '_' || ldr.list_val_char ||
                              --  ro.siteid ADDWORKCENTERNAME,
                              rc.eng_res_nm ADDWORKCENTERNAME,
                              ra.uph        PRODRATE
                FROM (select distinct enterprise,
                                      eng_rte_id,
                                      eng_oper_seq,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area
                        from RES_RTE_ASN_DTL_SRC
                      --   where alt_res_flg = 'Y'  and alt_rte_flg = 'N'
                       where alt_rte_flg = 'N'
                         and alt_res_flg = 'N'
                         and rownum >= 1) dtl,
                     routingoperation_stg ro,
                     opresource_stg odsopres,
                     scmdata.res_attr_src ra,
                     (select distinct eng_rte_id,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_PART_ASN_SRC
                       where rownum >= 1) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_CAP_SRC
                       where rownum >= 1) rc
               WHERE ro.routingid = dtl.eng_rte_id
                 AND ro.operationseq = dtl.eng_oper_seq
                    --  AND ro.siteid = rc.eng_facility
                 AND ro.routingid = odsopres.routingid
                    --  and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                 AND ro.operationseq = odsopres.operationseq
                 AND ra.prim_res_set_id = pra.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND ro.siteid = odsopres.siteid
                 AND ra.res_type IN ('HANDLER')
                 AND ra.res_nm is NOT NULL
                 AND odsopres.resourcename not like 'BLIN%'
                 AND ra.res_nm = rc.res_nm
                 AND rc.res_area = pra.res_area
                 and rc.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 AND ro.siteid = rc.site_num
                 AND ra.prim_res_set_id = ra.res_set_id
                 AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND ra.prim_res_set_id = dtl.prim_res_set_id
                 AND eer.enterprise <> 'SCP_STARTS' --Added by SA on 12/20
                 and CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                    --AND   ro.enterprise    = rc.enterprise
                 AND eer.enterprise = ro.enterprise
                 AND odsopres.enterprise = ro.enterprise --added by SA on 1/9/2018
                 AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND rc.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND eer.enterprise = i_enterprise_nm); --\* op*\
    commit;
    -- Simultaneous resource HW_SET_ID's
    DBMS_OUTPUT.PUT_LINE(2 || '-' || SYSTIMESTAMP || i_enterprise_nm);
    INSERT INTO OPRESOURCEADD_STG
      (ENGINE_ID,
       ENTERPRISE,
       OPERATION, -- ROUTINGHEADER>OPERATION
       OPERATIONSEQ,
       RESOURCENAME,
       ROUTINGID,
       SITEID,
       WORKCENTERNAME,
       sourcedate,
       ADDRESOURCENAME,
       ADDRESOURCESITEID, -- hardcode as 'RES_SITE'
       ADDWORKCENTERNAME,
       PRODRATE)
      SELECT *
        FROM (SELECT distinct eer.engine_id,
                              eer.enterprise,
                              ro.operation,
                              ro.operationseq,
                              odsopres.resourcename, -- resourcename
                              odsopres.routingid,
                              odsopres.siteid,
                              odsopres.workcentername,
                              sysdate,
                              -- ra.hardware_name || '_' || ldr.list_val_char ||
                              -- ro.siteid ADDRESOURCENAME,
                              rc.eng_res_nm ADDRESOURCENAME,
                              'RES_SITE' as ADDRESOURCESITEID,
                              rc.eng_res_nm ADDWORKCENTERNAME,
                              ra.uph PRODRATE
                FROM (select distinct enterprise,
                                      eng_rte_id,
                                      eng_oper_seq,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area
                        from RES_RTE_ASN_DTL_SRC dtl
                       where alt_rte_flg = 'N'
                         and alt_res_flg = 'N'
                         and rownum >= 1) dtl,
                     routingoperation_stg ro,
                     opresource_stg odsopres,
                     scmdata.res_attr_src ra,
                     (select distinct eng_rte_id,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area,
                                      enterprise --added by sa on 1/9
                        from RES_PART_ASN_SRC
                       where rownum >= 1) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_CAP_SRC
                       where rownum >= 1) rc
               WHERE ro.routingid = dtl.eng_rte_id
                 AND ro.operationseq = dtl.eng_oper_seq
                    --  AND ro.siteid = rc.eng_facility
                    --   and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                 AND ro.routingid = odsopres.routingid
                 AND ro.operationseq = odsopres.operationseq
                 AND ra.prim_res_set_id = pra.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND ro.siteid = odsopres.siteid
                 AND ra.hw_nm = rc.res_nm
                 AND rc.res_area = pra.res_area
                 and rc.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 AND ro.siteid = rc.site_num
                 AND ra.res_type = 'HARDWARE_SET'
                 AND odsopres.resourcename not like 'BLIN%'
                 AND ra.hw_nm is NOT NULL
                 AND ra.prim_res_set_id = ra.res_set_id
                 AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND ra.prim_res_set_id = dtl.prim_res_set_id
                 and CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND eer.enterprise <> 'SCP_STARTS' --Added by SA on 12/20
                 AND eer.enterprise = ro.enterprise
                 AND odsopres.enterprise = ro.enterprise --added by SA on 1/9/2018
                 AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND rc.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND eer.enterprise = i_enterprise_nm); --\*) op --149 of 248 total  -- 268 of 296  alt_res_flg = Y                                                                                           WHERE EXISTS (SELECT NULL
    commit;
    -- simultaneous resources HANDLERS
    DBMS_OUTPUT.PUT_LINE(3 || '-' || SYSTIMESTAMP || i_enterprise_nm);
    INSERT INTO OPRESOURCEADD_STG
      (ENGINE_ID,
       ENTERPRISE,
       OPERATION, -- ROUTINGHEADER>OPERATION
       OPERATIONSEQ,
       RESOURCENAME,
       ROUTINGID,
       SITEID,
       WORKCENTERNAME,
       sourcedate,
       ADDRESOURCENAME,
       ADDRESOURCESITEID, -- hardcode as 'RES_SITE'
       ADDWORKCENTERNAME,
       PRODRATE)
      SELECT *
        FROM (SELECT distinct eer.engine_id,
                              eer.enterprise,
                              ro.operation,
                              ro.operationseq,
                              odsopres.resourcename, -- resourcename
                              odsopres.routingid,
                              odsopres.siteid,
                              odsopres.workcentername,
                              sysdate,
                              --  pra.res_area,
                              --  ldr.list_val_char,
                              rc.eng_res_nm ADDRESOURCENAME,
                              'RES_SITE' as ADDRESOURCESITEID,
                              rc.eng_res_nm ADDWORKCENTERNAME,
                              ra.uph PRODRATE
                FROM (select distinct enterprise,
                                      eng_rte_id,
                                      eng_oper_seq,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area
                        from RES_RTE_ASN_DTL_SRC dtl
                      --   where alt_res_flg = 'Y'  and alt_rte_flg = 'N'
                       where alt_rte_flg = 'N'
                         and alt_res_flg = 'Y'
                         and rownum >= 1) dtl,
                     routingoperation_stg ro,
                     opresource_stg odsopres,
                     scmdata.res_attr_src ra,
                     (select distinct eng_rte_id,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_PART_ASN_SRC
                       where rownum >= 1) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_CAP_SRC
                       where rownum >= 1) rc
               WHERE ro.routingid = dtl.eng_rte_id
                 AND ro.operationseq = dtl.eng_oper_seq
                 AND ro.routingid = odsopres.routingid
                    --and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                 AND ro.operationseq = odsopres.operationseq
                 AND ra.prim_res_set_id = pra.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND ro.siteid = odsopres.siteid
                 AND ra.res_nm = rc.res_nm
                 AND rc.res_area = pra.res_area
                 and rc.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 AND ro.siteid = rc.site_num
                 AND ra.res_type IN ('HANDLER')
                 AND ra.res_nm is NOT NULL
                 AND odsopres.resourcename not like 'BLIN%'
                 AND ra.prim_res_set_id = ra.res_set_id
                 AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND ra.prim_res_set_id = dtl.prim_res_set_id
                    --AND   ro.enterprise    = rc.enterprise
                 and CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND eer.enterprise <> 'SCP_STARTS' --Added by SA on 12/20
                 AND odsopres.enterprise = ro.enterprise --added by SA on 1/9/2018
                 AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND rc.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND eer.enterprise = ro.enterprise
                 AND eer.enterprise = i_enterprise_nm);
    commit;
    -- Simultaneous resource HW_SET_ID's
    DBMS_OUTPUT.PUT_LINE(4 || '-' || SYSTIMESTAMP || i_enterprise_nm);
    INSERT INTO OPRESOURCEADD_STG
      (ENGINE_ID,
       ENTERPRISE,
       OPERATION, -- ROUTINGHEADER>OPERATION
       OPERATIONSEQ,
       RESOURCENAME,
       ROUTINGID,
       SITEID,
       WORKCENTERNAME,
       sourcedate,
       ADDRESOURCENAME,
       ADDRESOURCESITEID, -- hardcode as 'RES_SITE'
       ADDWORKCENTERNAME,
       PRODRATE)
      SELECT *
        FROM (SELECT distinct eer.engine_id,
                              eer.enterprise,
                              ro.operation,
                              ro.operationseq,
                              odsopres.resourcename, -- resourcename
                              odsopres.routingid,
                              odsopres.siteid,
                              odsopres.workcentername,
                              sysdate,
                              
                              rc.eng_res_nm ADDRESOURCENAME,
                              'RES_SITE' as ADDRESOURCESITEID,
                              rc.eng_res_nm ADDWORKCENTERNAME,
                              
                              ra.uph PRODRATE
                FROM (select distinct enterprise,
                                      eng_rte_id,
                                      eng_oper_seq,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area
                        from RES_RTE_ASN_DTL_SRC dtl
                       where alt_rte_flg = 'N'
                         and alt_res_flg = 'Y'
                         and rownum >= 1) dtl,
                     routingoperation_stg ro,
                     opresource_stg odsopres,
                     scmdata.res_attr_src ra,
                     (select distinct eng_rte_id,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_PART_ASN_SRC
                       where rownum >= 1) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_CAP_SRC
                       where rownum >= 1) rc
               WHERE ro.routingid = dtl.eng_rte_id
                 AND ro.operationseq = dtl.eng_oper_seq
                    --  AND ro.siteid = rc.eng_facility
                 AND ro.routingid = odsopres.routingid
                    --   and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                 AND ro.operationseq = odsopres.operationseq
                 AND ra.prim_res_set_id = pra.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND ro.siteid = odsopres.siteid
                 AND ra.res_type = 'HARDWARE_SET'
                 AND ra.hw_nm is NOT NULL
                 AND odsopres.resourcename not like 'BLIN%'
                 AND ra.hw_nm = rc.res_nm
                 AND rc.res_area = pra.res_area
                 and rc.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 AND ro.siteid = rc.site_num
                 AND ra.res_type = 'HARDWARE_SET'
                 AND ra.prim_res_set_id = ra.res_set_id
                 AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND ra.prim_res_set_id = dtl.prim_res_set_id
                 and CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND eer.enterprise <> 'SCP_STARTS' --Added by SA on 12/20
                 AND odsopres.enterprise = ro.enterprise --added by SA on 1/9/2018
                 AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND rc.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND eer.enterprise = ro.enterprise
                 AND eer.enterprise = i_enterprise_nm); -- op --149 of 248 total  -- 268 of 296  alt_res_flg = Y
    commit;
    --    ALT_RTE_FLG= 'Y'
    -- add simultaneous resources HANDLERS
    DBMS_OUTPUT.PUT_LINE(5 || '-' || SYSTIMESTAMP || i_enterprise_nm);
    INSERT INTO OPRESOURCEADD_STG
      (ADDRESOURCENAME,
       ADDRESOURCESITEID,
       ADDWORKCENTERNAME,
       ROUTINGID,
       SITEID,
       ENTERPRISE,
       ENGINE_ID,
       OPERATION,
       OPERATIONSEQ,
       PRODRATE,
       RESOURCENAME,
       WORKCENTERNAME,
       sourcedate)
      select *
        from (SELECT distinct rc.eng_res_nm ADDRESOURCENAME, -- 529 out of 574 expected rows
                              'RES_SITE', --  -- hardcode as 'RES_SITE'
                              rc.eng_res_nm ADDWORKCENTERNAME,
                              oper.routingid,
                              oper.siteid,
                              eer.enterprise,
                              eer.engine_id,
                              oper.operation,
                              oper.operationseq,
                              attr.uph,
                              (select attr.res_nm || '_' || ldr.list_val_char ||
                                      oper1.siteid
                                 from RoutingOperation_STG oper1,
                                      RES_RTE_ALT_SRC alt1,
                                      RES_RTE_ASN_DTL_SRC dtl,
                                      RES_ATTR_SRC attr,
                                      (select distinct eng_rte_id,
                                                       prod_rte_seq_id,
                                                       prim_res_set_id,
                                                       res_area,
                                                       enterprise --added by SA on 1/9
                                         from RES_PART_ASN_SRC
                                        where rownum >= 1) pra,
                                      ref_data.list_dtl_ref ldr
                                where oper1.routingid = alt1.ENG_ALT_RTE_ID
                                  and oper1.operationseq = alt1.ENG_OPER_SEQ
                                  and CASE
                                        WHEN i_enterprise_nm LIKE 'OP%' THEN
                                         'SCP_DAILY'
                                        ELSE
                                         i_enterprise_nm
                                      END = dtl.enterprise --added by SA on 1/9/2018
                                  AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                                  AND alt1.enterprise = pra.enterprise --added by SA on 1/9/2018
                                  AND i_enterprise_nm = oper1.enterprise --added by SA on 1/9/2018
                                  AND oper.enterprise = oper1.enterprise --added by SA on 1/9/2018
                                  and alt1.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                                     /*and oper1.routingid not like
                                     'MAX1480AEPI+_S_4900%' */ --- should remove this later
                                  and alt1.res_area <> 'NON ATE FT'
                                  and alt1.res_area = dtl.res_area
                                  and dtl.res_area = pra.res_area
                                  and alt1.res_set_id =
                                      dtl.alt_rte_res_set_id
                                  and alt1.ENG_OPER_SEQ = dtl.eng_oper_seq
                                  and dtl.prod_rte_seq_id =
                                      attr.prod_rte_seq_id
                                  AND pra.res_area = ldr.list_dtl_num
                                  AND pra.prod_rte_seq_id =
                                      dtl.prod_rte_seq_id
                                  AND pra.prim_res_set_id =
                                      dtl.prim_res_set_id
                                  and dtl.prim_res_set_id =
                                      attr.prim_res_set_id
                                  and dtl.alt_rte_res_set_id =
                                      attr.res_set_id
                                  and attr.res_type = 'TESTER'
                                  AND attr.res_nm not like 'BLIN%'
                                     --and     alt1.ENG_ORIG_RTE_ID      = 'HS45Z_S1_2_4905_1'
                                  and alt1.ENG_ALT_RTE_ID = oper.routingid
                                  and oper1.operationseq = oper.operationseq
                                  and rownum >= 1) RESOURCENAME,
                              (select attr.res_nm || '_' || ldr.list_val_char ||
                                      oper1.siteid
                                 from RoutingOperation_STG oper1,
                                      RES_RTE_ALT_SRC alt1,
                                      RES_RTE_ASN_DTL_SRC dtl,
                                      RES_ATTR_SRC attr,
                                      (select distinct eng_rte_id,
                                                       prod_rte_seq_id,
                                                       prim_res_set_id,
                                                       res_area,
                                                       enterprise --added by SA on 1/9
                                         from RES_PART_ASN_SRC
                                        where rownum >= 1) pra,
                                      ref_data.list_dtl_ref ldr
                                where oper1.routingid = alt1.ENG_ALT_RTE_ID
                                  and oper1.operationseq = alt1.ENG_OPER_SEQ
                                     /* and oper1.routingid not like
                                     'MAX1480AEPI+_S_4900%'*/ --- should remove this later
                                  and alt1.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                                  and alt1.res_area <> 'NON ATE FT'
                                  and alt1.res_area = dtl.res_area
                                  and dtl.res_area = pra.res_area
                                  and alt1.res_set_id =
                                      dtl.alt_rte_res_set_id
                                  and alt1.ENG_OPER_SEQ = dtl.eng_oper_seq
                                  and dtl.prod_rte_seq_id =
                                      attr.prod_rte_seq_id
                                  AND pra.res_area = ldr.list_dtl_num
                                  AND pra.prod_rte_seq_id =
                                      dtl.prod_rte_seq_id
                                  AND pra.prim_res_set_id =
                                      dtl.prim_res_set_id
                                  and dtl.prim_res_set_id =
                                      attr.prim_res_set_id
                                  and dtl.alt_rte_res_set_id =
                                      attr.res_set_id
                                  and attr.res_type = 'TESTER'
                                  AND attr.res_nm not like 'BLIN%'
                                     --and     alt1.ENG_ORIG_RTE_ID      = 'HS45Z_S1_2_4905_1'
                                  and alt1.ENG_ALT_RTE_ID = oper.routingid
                                  and oper1.operationseq = oper.operationseq
                                  and CASE
                                        WHEN i_enterprise_nm LIKE 'OP%' THEN
                                         'SCP_DAILY'
                                        ELSE
                                         i_enterprise_nm
                                      END = dtl.enterprise --added by SA on 1/9/2018
                                  AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                                  AND alt1.enterprise = pra.enterprise --added by SA on 1/9/2018
                                  AND i_enterprise_nm = oper1.enterprise --added by SA on 1/9/2018
                                  AND oper.enterprise = oper1.enterprise --added by SA on 1/9/2018
                                  and rownum >= 1) WORKCENTERNAME,
                              sysdate
                FROM RoutingOperation_STG oper,
                     RES_RTE_ALT_SRC alt,
                     RES_RTE_ASN_DTL_SRC dtl,
                     RES_ATTR_SRC attr,
                     (select distinct eng_rte_id,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_PART_ASN_SRC
                       where rownum >= 1) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_CAP_SRC
                       where rownum >= 1) rc
               WHERE oper.routingid = alt.ENG_ALT_RTE_ID
                    --and    alt.ENG_ORIG_RTE_ID   = 'HS45Z_S1_2_4905_1'
                 AND oper.operationseq = alt.ENG_OPER_SEQ
                 AND oper.operationseq = alt.ENG_OPER_SEQ
                    --and oper.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                 AND alt.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                 AND alt.res_set_id = dtl.alt_rte_res_set_id
                 AND alt.ENG_OPER_SEQ = dtl.eng_oper_seq
                 AND dtl.prod_rte_seq_id = attr.prod_rte_seq_id
                 AND dtl.prim_res_set_id = attr.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND dtl.alt_rte_res_set_id = attr.res_set_id
                    --  AND attr.res_type = 'HW_SET_ID'
                    --  AND attr.hardware_name IS NOT NULL
                 AND attr.res_type = 'HANDLER'
                 AND attr.res_nm IS NOT NULL
                 AND attr.res_nm = rc.res_nm
                 AND rc.res_area = pra.res_area
                 and rc.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 and dtl.res_area = alt.res_area
                 AND oper.siteid = rc.site_num
                 AND oper.enterprise = alt.enterprise
                 AND alt.enterprise = dtl.enterprise
                 AND eer.enterprise <> 'SCP_STARTS' --Added by SA on 12/20
                 and CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND rc.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND rc.enterprise = alt.enterprise --added by SA on 1/9/2018
                 AND eer.enterprise = oper.enterprise
                 AND eer.enterprise = i_enterprise_nm);
    commit;
    -- Simultaneous resource HW_SET_ID's
    DBMS_OUTPUT.PUT_LINE(6 || '-' || SYSTIMESTAMP || i_enterprise_nm);
    INSERT INTO OPRESOURCEADD_STG
      (ADDRESOURCENAME,
       ADDRESOURCESITEID,
       ADDWORKCENTERNAME,
       ROUTINGID,
       SITEID,
       ENTERPRISE,
       ENGINE_ID,
       OPERATION,
       OPERATIONSEQ,
       PRODRATE,
       RESOURCENAME,
       WORKCENTERNAME,
       sourcedate)
      select *
        from (SELECT distinct rc.eng_res_nm ADDRESOURCENAME, -- 529 out of 574 expected rows
                              'RES_SITE', --  -- hardcode as 'RES_SITE'
                              rc.eng_res_nm ADDWORKCENTERNAME,
                              oper.routingid,
                              oper.siteid,
                              eer.enterprise,
                              eer.engine_id,
                              oper.operation,
                              oper.operationseq,
                              attr.uph,
                              (select attr.res_nm || '_' || ldr.list_val_char ||
                                      oper1.siteid
                                 FROM RoutingOperation_STG oper1,
                                      RES_RTE_ALT_SRC alt1,
                                      RES_RTE_ASN_DTL_SRC dtl,
                                      RES_ATTR_SRC attr,
                                      (select distinct eng_rte_id,
                                                       prod_rte_seq_id,
                                                       prim_res_set_id,
                                                       res_area
                                         from RES_PART_ASN_SRC
                                        where rownum >= 1) pra,
                                      ref_data.list_dtl_ref ldr
                                WHERE oper1.routingid = alt1.ENG_ALT_RTE_ID
                                  and oper1.operationseq = alt1.ENG_OPER_SEQ
                                  and alt1.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                                  and alt1.res_area <> 'NON ATE FT'
                                  and alt1.res_area = dtl.res_area
                                  and oper1.routingid not like
                                      'MAX1480AEPI+_S_4900%' --- should remove this later
                                  and dtl.res_area = pra.res_area
                                  and alt1.res_set_id =
                                      dtl.alt_rte_res_set_id
                                  and alt1.ENG_OPER_SEQ = dtl.eng_oper_seq
                                  and dtl.prod_rte_seq_id =
                                      attr.prod_rte_seq_id
                                  AND pra.res_area = ldr.list_dtl_num
                                  AND pra.prod_rte_seq_id =
                                      dtl.prod_rte_seq_id
                                  AND pra.prim_res_set_id =
                                      dtl.prim_res_set_id
                                  and dtl.prim_res_set_id =
                                      attr.prim_res_set_id
                                  and dtl.alt_rte_res_set_id =
                                      attr.res_set_id
                                  and rownum >= 1
                                  and attr.res_type = 'TESTER'
                                  AND attr.res_nm not like 'BLIN%'
                                     --and     alt1.ENG_ORIG_RTE_ID      = 'HS45Z_S1_2_4905_1'
                                  and alt1.ENG_ALT_RTE_ID = oper.routingid
                                  and oper1.operationseq = oper.operationseq
                                  and CASE
                                        WHEN i_enterprise_nm LIKE 'OP%' THEN
                                         'SCP_DAILY'
                                        ELSE
                                         i_enterprise_nm
                                      END = dtl.enterprise --added by SA on 1/9/2018
                                  AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                                  AND alt1.enterprise = pra.enterprise --added by SA on 1/9/2018
                                  AND i_enterprise_nm = oper1.enterprise --added by SA on 1/9/2018
                                  AND oper.enterprise = oper1.enterprise --added by SA on 1/9/2018
                               ) RESOURCENAME,
                              (select attr.res_nm || '_' || ldr.list_val_char ||
                                      oper1.siteid
                                 from RoutingOperation_STG oper1,
                                      RES_RTE_ALT_SRC alt1,
                                      RES_RTE_ASN_DTL_SRC dtl,
                                      RES_ATTR_SRC attr,
                                      (select distinct eng_rte_id,
                                                       prod_rte_seq_id,
                                                       prim_res_set_id,
                                                       res_area,
                                                       ENTERPRISE --added by SA on 1/9
                                         from RES_PART_ASN_SRC
                                        where rownum >= 1) pra,
                                      ref_data.list_dtl_ref ldr
                                WHERE oper1.routingid = alt1.ENG_ALT_RTE_ID
                                  AND oper1.operationseq = alt1.ENG_OPER_SEQ
                                     /*and oper1.routingid not like
                                     'MAX1480AEPI+_S_4900%'*/ --- should remove this later
                                  AND alt1.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                                  AND alt1.res_set_id =
                                      dtl.alt_rte_res_set_id
                                  and alt1.res_area <> 'NON ATE FT'
                                  and alt1.res_area = dtl.res_area
                                  and dtl.res_area = pra.res_area
                                  AND alt1.ENG_OPER_SEQ = dtl.eng_oper_seq
                                  AND dtl.prod_rte_seq_id =
                                      attr.prod_rte_seq_id
                                  AND pra.res_area = ldr.list_dtl_num
                                  AND pra.prod_rte_seq_id =
                                      dtl.prod_rte_seq_id
                                  AND pra.prim_res_set_id =
                                      dtl.prim_res_set_id
                                  AND dtl.prim_res_set_id =
                                      attr.prim_res_set_id
                                  AND dtl.alt_rte_res_set_id =
                                      attr.res_set_id
                                  AND attr.res_type = 'TESTER'
                                  AND attr.res_nm not like 'BLIN%'
                                     --and     alt1.ENG_ORIG_RTE_ID      = 'HS45Z_S1_2_4905_1'
                                  AND alt1.ENG_ALT_RTE_ID = oper.routingid
                                  AND oper1.operationseq = oper.operationseq
                                  and CASE
                                        WHEN i_enterprise_nm LIKE 'OP%' THEN
                                         'SCP_DAILY'
                                        ELSE
                                         i_enterprise_nm
                                      END = dtl.enterprise --added by SA on 1/9/2018
                                  AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                                  AND alt1.enterprise = pra.enterprise --added by SA on 1/9/2018
                                  AND i_enterprise_nm = oper1.enterprise --added by SA on 1/9/2018
                                  AND oper.enterprise = oper1.enterprise --added by SA on 1/9/2018
                                  and rownum >= 1) WORKCENTERNAME,
                              sysdate
                FROM RoutingOperation_STG oper,
                     RES_RTE_ALT_SRC alt,
                     RES_RTE_ASN_DTL_SRC dtl,
                     RES_ATTR_SRC attr,
                     (select distinct eng_rte_id,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_PART_ASN_SRC
                       where rownum >= 1) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      res_area,
                                      enterprise --added by SA on 1/9
                        from RES_CAP_SRC
                       where rownum >= 1) rc
               WHERE oper.routingid = alt.ENG_ALT_RTE_ID
                    --and    alt.ENG_ORIG_RTE_ID   = 'HS45Z_S1_2_4905_1'
                 AND oper.operationseq = alt.ENG_OPER_SEQ
                    --and oper.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                 AND oper.operationseq = alt.ENG_OPER_SEQ
                 AND alt.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                 AND attr.hw_nm = rc.res_nm
                 AND rc.res_area = pra.res_area
                 and rc.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 AND oper.siteid = rc.site_num
                 AND alt.res_set_id = dtl.alt_rte_res_set_id
                 AND alt.ENG_OPER_SEQ = dtl.eng_oper_seq
                 AND dtl.prod_rte_seq_id = attr.prod_rte_seq_id
                 AND dtl.prim_res_set_id = attr.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND dtl.alt_rte_res_set_id = attr.res_set_id
                 AND attr.res_type = 'HARDWARE_SET'
                 AND attr.hw_nm IS NOT NULL
                    -- AND attr.res_type = 'HANDLER'
                    --  AND attr.res_nm IS NOT NULL
                 AND eer.enterprise <> 'SCP_STARTS' --Added by SA on 12/20
                 and CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND rc.enterprise = pra.enterprise --added by SA on 1/9/2018
                 AND rc.enterprise = alt.enterprise --added by SA on 1/9/2018
                 AND eer.enterprise = oper.enterprise
                 and oper.routingid not in
                     ('MAX16984RAGI/VY+_T1_S_4905_1_ALT_2',
                      'MAX16984SAGI/VY+_T1_S_4905_1_ALT_2')
                 AND eer.enterprise = i_enterprise_nm);
    commit;
    DBMS_OUTPUT.PUT_LINE(7 || '-' || SYSTIMESTAMP || i_enterprise_nm);
    ---------populated for Assembly resources------
    -------only res_grp_cd will go to opresourceadd-----------
    insert into opresourceadd_stg
      (ENGINE_ID,
       ENTERPRISE,
       OPERATION,
       OPERATIONSEQ,
       RESOURCENAME,
       ROUTINGID,
       SITEID,
       WORKCENTERNAME,
       sourcedate,
       ADDRESOURCENAME,
       ADDRESOURCESITEID,
       ADDWORKCENTERNAME,
       PRODRATE)
      select distinct eer.engine_id,
                      eer.enterprise,
                      ro.operation,
                      ro.operationseq,
                      os.resourcename,
                      ro.routingid,
                      os.siteid,
                      os.workcentername,
                      sysdate,
                      rcs.eng_res_nm addresourcename,
                      'RES_SITE' addresourcesiteid,
                      rcs.eng_res_nm addworkcentername,
                      os.prodrate
        from res_cap_src                 rcs,
             assy_part_vnd_src           apv,
             opresource_stg              os,
             routingoperation_stg        ro,
             ref_data.eng_enterprise_ref eer
       where rcs.enterprise = apv.enterprise
         and eer.enterprise = ro.enterprise
         and ro.enterprise = os.enterprise
         and CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = apv.enterprise
         and eer.enterprise = i_enterprise_nm
         and eer.enterprise <> 'SCP_STARTS'
         and rcs.res_nm = apv.res_grp_cd
         and rcs.res_type = apv.res_cd
         and apv.vnd_num = rcs.vnd_num
         and ro.routingid like '%' || apv.eng_rte_id || '%'
         and apv.eng_oper_seq = ro.operationseq
         and apv.eng_site_num = rcs.site_num
         and apv.eng_site_num = ro.siteid
         and ro.routingid = os.routingid
         and os.operationseq = ro.operationseq
         and os.operation = ro.operation;
  
    ---------Populate data for NON ATE-------
    INSERT INTO OPRESOURCEADD_STG
      (ENGINE_ID,
       ENTERPRISE,
       OPERATION,
       OPERATIONSEQ,
       ROUTINGID,
       SITEID,
       sourcedate,
       ADDRESOURCENAME,
       ADDRESOURCESITEID,
       ADDWORKCENTERNAME,
       PRODRATE,
       RESOURCENAME,
       WORKCENTERNAME)
    --alt_res_flg = 'N' and alt_rte_flg = 'N'
      SELECT distinct eer.engine_id,
                      eer.enterprise,
                      ro.operation,
                      ro.operationseq,
                      odsopres.routingid,
                      odsopres.siteid,
                      sysdate,
                      rc.eng_res_nm ADDRESOURCENAME,
                      'RES_SITE' as ADDRESOURCESITEID,
                      rc.eng_res_nm ADDWORKCENTERNAME,
                      null PRODRATE,
                      odsopres.resourcename, -- resourcename
                      odsopres.workcentername
        FROM (select distinct enterprise,
                              eng_rte_id,
                              eng_oper_seq,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area
                from RES_RTE_ASN_DTL_SRC
               where alt_rte_flg = 'N'
                 and alt_res_flg = 'N'
                 and res_area = 'NON ATE FT') dtl,
             routingoperation_stg ro,
             opresource_stg odsopres,
             scmdata.res_nonate_attr_src ra,
             (select distinct eng_rte_id,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area,
                              enterprise
                from RES_PART_ASN_SRC
               where res_area = 'NON ATE FT') pra,
             ref_data.list_dtl_ref ldr,
             ref_data.eng_enterprise_ref eer,
             (select distinct res_nm,
                              site_num,
                              eng_res_nm,
                              res_area,
                              enterprise
                from RES_CAP_SRC
               where res_area like 'NON%') rc
       WHERE ro.routingid = dtl.eng_rte_id
         AND ro.operationseq = dtl.eng_oper_seq
         AND ro.routingid = odsopres.routingid
         AND ro.operationseq = odsopres.operationseq
         AND ra.prim_res_set_id = pra.prim_res_set_id
         AND pra.res_area = ldr.list_dtl_num
         AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND pra.prim_res_set_id = dtl.prim_res_set_id
         AND ro.siteid = odsopres.siteid
         AND ra.res_type = 'BIBR'
         AND ra.res_nm is NOT NULL
         AND ra.res_nm = rc.res_nm
         AND rc.res_area like '%' || pra.res_area || '%'
         and pra.res_area = dtl.res_area
         AND ro.siteid = rc.site_num
         AND ra.prim_res_set_id = ra.res_set_id
         AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND ra.prim_res_set_id = dtl.prim_res_set_id
         AND eer.enterprise <> 'SCP_STARTS'
         and CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = dtl.enterprise
         and dtl.enterprise = pra.enterprise
         and pra.enterprise = ra.enterprise
         and ra.enterprise = rc.enterprise
         AND eer.enterprise = odsopres.enterprise --added by SA on 1/9
         AND odsopres.enterprise = ro.enterprise
            -- and rc.enterprise = odsopres.enterprise
         AND eer.enterprise = i_enterprise_nm
      union all --alt_rte_flg= 'N' and alt_res_flg = 'Y'
      SELECT distinct eer.engine_id,
                      eer.enterprise,
                      ro.operation,
                      ro.operationseq,
                      odsopres.routingid,
                      odsopres.siteid,
                      sysdate,
                      rc.eng_res_nm ADDRESOURCENAME,
                      'RES_SITE' as ADDRESOURCESITEID,
                      rc.eng_res_nm ADDWORKCENTERNAME,
                      null PRODRATE,
                      odsopres.resourcename,
                      odsopres.workcentername
        FROM (select distinct enterprise,
                              eng_rte_id,
                              eng_oper_seq,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area
                from RES_RTE_ASN_DTL_SRC dtl
               where alt_rte_flg = 'N'
                 and alt_res_flg = 'Y'
                 and res_area = 'NON ATE FT') dtl,
             routingoperation_stg ro,
             opresource_stg odsopres,
             scmdata.res_nonate_attr_src ra,
             (select distinct eng_rte_id,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area,
                              enterprise
                from RES_PART_ASN_SRC
               where res_area = 'NON ATE FT') pra,
             ref_data.list_dtl_ref ldr,
             ref_data.eng_enterprise_ref eer,
             (select distinct res_nm,
                              site_num,
                              eng_res_nm,
                              res_area,
                              enterprise
                from RES_CAP_SRC
               where res_area like 'NON%') rc
       WHERE ra.res_type = 'BIBR'
         and ro.routingid = dtl.eng_rte_id
         AND ro.operationseq = dtl.eng_oper_seq
         AND ro.routingid = odsopres.routingid
         AND ro.operationseq = odsopres.operationseq
         AND ra.prim_res_set_id = pra.prim_res_set_id
         AND pra.res_area = ldr.list_dtl_num
         AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND pra.prim_res_set_id = dtl.prim_res_set_id
         AND ro.siteid = odsopres.siteid
         AND ra.res_nm is NOT NULL
         AND ra.res_nm = rc.res_nm
         AND rc.res_area like '%' || pra.res_area || '%'
         and pra.res_area = dtl.res_area
         AND ro.siteid = rc.site_num
         AND ra.prim_res_set_id = ra.res_set_id
         AND ra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND ra.prim_res_set_id = dtl.prim_res_set_id
         AND dtl.enterprise = ro.enterprise
         and CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = dtl.enterprise
         and dtl.enterprise = pra.enterprise
         and pra.enterprise = ra.enterprise
         and ra.enterprise = rc.enterprise
         AND eer.enterprise = odsopres.enterprise --added by SA on 1/9
         AND odsopres.enterprise = ro.enterprise
            -- and rc.enterprise = odsopres.enterprise
         AND eer.enterprise = i_enterprise_nm
      union all --    ALT_RTE_FLG= 'Y'
      SELECT distinct eer.engine_id,
                      eer.enterprise,
                      oper.operation,
                      oper.operationseq,
                      oper.routingid,
                      oper.siteid,
                      sysdate,
                      rc.eng_res_nm ADDRESOURCENAME,
                      'RES_SITE',
                      rc.eng_res_nm ADDWORKCENTERNAME,
                      null uph,
                      attr.res_nm || '_' || ldr.list_val_char || '_' ||
                      oper.siteid RESOURCENM,
                      attr.res_nm || '_' || ldr.list_val_char || '_' ||
                      oper.siteid WORKCENTERNM
        FROM RoutingOperation_STG oper,
             RES_RTE_ALT_SRC alt,
             RES_RTE_ASN_DTL_SRC dtl,
             RES_nonate_ATTR_SRC attr,
             (select distinct eng_rte_id,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area,
                              enterprise
                from RES_PART_ASN_SRC
               where res_area = 'NON ATE FT') pra,
             ref_data.list_dtl_ref ldr,
             ref_data.eng_enterprise_ref eer,
             (select distinct res_nm,
                              site_num,
                              eng_res_nm,
                              res_area,
                              enterprise
                from RES_CAP_SRC
               where res_area like 'NON%') rc
       WHERE alt.ENG_ALT_RTE_ID like '%' || oper.routingid || '%'
         AND oper.operationseq = alt.ENG_OPER_SEQ
         AND oper.operationseq = alt.ENG_OPER_SEQ
         AND alt.ENG_ORIG_RTE_ID = dtl.eng_rte_id
         AND alt.res_set_id = dtl.alt_rte_res_set_id
         AND alt.ENG_OPER_SEQ = dtl.eng_oper_seq
         AND dtl.prod_rte_seq_id = attr.prod_rte_seq_id
         AND dtl.prim_res_set_id = attr.prim_res_set_id
         AND pra.res_area = ldr.list_dtl_num
         AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND pra.prim_res_set_id = dtl.prim_res_set_id
         AND dtl.alt_rte_res_set_id = attr.res_set_id
         AND attr.res_type = 'BIBR'
         AND attr.res_nm IS NOT NULL
         AND attr.res_nm = rc.res_nm
         AND rc.res_area like '%' || pra.res_area || '%'
         and pra.res_area = dtl.res_area
         and dtl.res_area = alt.res_area
         AND oper.siteid = rc.site_num
         AND dtl.enterprise = pra.enterprise --added by SA on 1/9/2018
         AND rc.enterprise = pra.enterprise --added by SA on 1/9/2018
         AND rc.enterprise = alt.enterprise --added by SA on 1/9/2018
         AND eer.enterprise = oper.enterprise
         and CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = dtl.enterprise
         AND eer.enterprise = i_enterprise_nm;
  
    --END IF;
    Commit; /* remove later  */
    frmdata.logs.info('OPRESOURCEADD_STG load from SRC to ODS is completed');
  
  END load_OPRESOURCEADD;

  --************************************************************
  --Procedure to load data into OPRESOURCEALT table -9
  -- This procedure adds TESTERS, HANDLERS and HW_SET_ID's  in OPRESOURCEALT table for ALL alterante resource set ids
  -- This procedure adds data for rows meeting ALT_RES_FLG= 'Y' criteria
  -- Adding data for Hardware Alternates in RES_HW_ALT_SRC

  --************************************************************
  PROCEDURE load_OPRESOURCEALT(i_enterprise_nm IN VARCHAR2) IS
  
    --  lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date
  
  BEGIN
  
    frmdata.logs.begin_log('Start OPRESOURCEALT load from SRC to ODS');
    frmdata.delete_data('OPRESOURCEALT_STG',
                        'enterprise = ' || '''' || i_enterprise_nm || '''',
                        10000);
  
    -- lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm);
  
    -- For alt_res_flg = 'Y' case TESTERS
    -------------ATE-----------------------
    INSERT INTO OPRESOURCEALT_STG
      (ALTRESOURCENAME, -- get alternate resources ie. tester,handler,hw_set_id
       ALTRESOURCESITEID, -- 'RES_SITE'
       ALTWORKCENTERNAME, -- get alternate resources ie. tester,handler,hw_set_id
       PRODRATE, -- -- get alternate resources ie. tester,handler,hw_set_id uph
       ENGINE_ID,
       ENTERPRISE,
       OPERATION,
       OPERATIONSEQ,
       ROUTINGID,
       SITEID,
       RESOURCENAME,
       WORKCENTERNAME,
       sourcedate)
      SELECT *
        FROM (SELECT DISTINCT rc1.eng_res_nm ALTRESOURCENAME,
                              'RES_SITE' ALTRESOURCESITEID, -- 'RES_SITE'
                              rc1.eng_res_nm ALTWORKCENTERNAME,
                              dtl.alt_res_uph,
                              eer.engine_id,
                              eer.enterprise,
                              ro.operation,
                              ro.operationseq,
                              ro.routingid,
                              ro.siteid, -- dtl.*,
                              odsopres.resourcename RESOURCENAME,
                              odsopres.workcentername WORKCENTERNAME,
                              sysdate
                FROM RES_RTE_ASN_SRC rc,
                     routingoperation_stg ro,
                     scmdata.RES_RTE_ASN_DTL_SRC dtl,
                     (select distinct eng_rte_id,
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area,
                                      enterprise --added by sa on 1/08
                        from RES_PART_ASN_SRC) pra,
                     ref_data.list_dtl_ref ldr,
                     opresource_stg odsopres,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      res_area,
                                      enterprise --added by sa on 1/08
                        from RES_CAP_SRC) rc1
               WHERE rc.alt_res_flg = 'Y'
                    --and   ro.enterprise    = rc.enterprise
                 AND ro.routingid = rc.eng_rte_id
                 AND ro.operationseq = rc.eng_oper_seq
                 AND ro.siteid = rc.eng_site_num
                 AND eer.enterprise = ro.enterprise
                    --   and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                 AND dtl.enterprise = rc.enterprise
                 AND rc1.enterprise = rc.enterprise --added by sa on 1/8
                 AND dtl.enterprise = pra.enterprise --added by sa on 1/8
                 and CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND eer.enterprise = i_enterprise_nm
                 AND rc.eng_rte_id = dtl.eng_rte_id
                 AND rc.eng_oper_seq = dtl.eng_oper_seq
                 AND dtl.alt_res_type = 'TESTER'
                 AND nvl(dtl.alt_res_nm, 'dflt') != dtl.alt_res_prim_res_nm ---adding nvl to dtl.alt_res_nm to void nulls Added by VS 12/20/17
                 AND dtl.alt_res_flg = 'Y'
                 AND dtl.alt_res_nm = rc1.res_nm -----changed the join from dtl.alt_res_nm = rc1.res_area to dtl.alt_res_nm = rc1.res_nm  added by VS 12/20/17
                 AND rc1.res_area = pra.res_area
                 and rc1.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc1.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 and dtl.res_area = rc.res_area
                 AND ro.siteid = rc1.site_num
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND odsopres.resourcename not like 'BLIN%'
                 AND odsopres.routingid = ro.routingid
                 AND odsopres.operationseq = ro.operationseq
                 AND odsopres.siteid = ro.siteid
                 AND odsopres.enterprise = ro.enterprise
              --   AND odsopres.engine_id = eer.engine_id  --NOT NEEDED
              );
  
    --   For alt_res_flg = 'Y' case HANDLERS
  
    INSERT INTO OPRESOURCEALT_STG
      (ALTRESOURCENAME, -- get alternate resources ie. tester,handler,hw_set_id
       ALTRESOURCESITEID, -- 'RES_SITE'
       ALTWORKCENTERNAME, -- get alternate resources ie. tester,handler,hw_set_id
       PRODRATE, -- -- get alternate resources ie. tester,handler,hw_set_id uph
       ENGINE_ID,
       ENTERPRISE,
       OPERATION,
       OPERATIONSEQ,
       ROUTINGID,
       SITEID,
       RESOURCENAME,
       WORKCENTERNAME,
       sourcedate)
      SELECT *
        FROM (SELECT DISTINCT -- dtl.alt_res_resource_nm || '_' || ro.siteid ALTRESOURCENAME, -- get alternate resources ie. tester,handler,hw_set_id
                              rc1.eng_res_nm ALTRESOURCENAME,
                              'RES_SITE' ALTRESOURCESITEID, -- 'RES_SITE'
                              rc1.eng_res_nm ALTWORKCENTERNAME,
                              dtl.alt_res_uph,
                              eer.engine_id,
                              eer.enterprise,
                              ro.operation,
                              ro.operationseq,
                              ro.routingid,
                              ro.siteid, -- dtl.*,
                              odsopres.addresourcename RESOURCENAME,
                              odsopres.addworkcentername WORKCENTERNAME,
                              sysdate
                FROM RES_RTE_ASN_SRC rc,
                     routingoperation_stg ro,
                     scmdata.RES_RTE_ASN_DTL_SRC dtl,
                     (select distinct eng_rte_id,
                                      enterprise, --added by SA on 1/8
                                      prod_rte_seq_id,
                                      prim_res_set_id,
                                      res_area
                        from RES_PART_ASN_SRC) pra,
                     ref_data.list_dtl_ref ldr,
                     opresourceadd_stg odsopres,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      res_area,
                                      enterprise --added by sa on 1/8
                        from RES_CAP_SRC) rc1
               WHERE rc.alt_res_flg = 'Y'
                    --and   ro.enterprise    = rc.enterprise
                 AND ro.routingid = rc.eng_rte_id
                 AND ro.operationseq = rc.eng_oper_seq
                 AND ro.siteid = rc.eng_site_num
                    -- and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                 AND eer.enterprise = ro.enterprise
                 AND dtl.enterprise = rc.enterprise
                 AND rc1.enterprise = rc.enterprise --added by sa on 1/8
                 AND dtl.enterprise = pra.enterprise --added by sa on 1/8
                    
                 and CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND eer.enterprise = i_enterprise_nm
                 AND rc.eng_rte_id = dtl.eng_rte_id
                 AND rc.eng_oper_seq = dtl.eng_oper_seq
                 AND dtl.alt_res_type = 'HANDLER'
                 AND nvl(dtl.alt_res_nm, 'dflt') != dtl.alt_res_prim_res_nm ---adding nvl to dtl.alt_res_nm to void nulls Added by VS 12/20/17
                 AND dtl.alt_res_flg = 'Y'
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND odsopres.resourcename not like 'BLIN%'
                 AND dtl.alt_res_nm = rc1.res_nm -----changed the join from dtl.alt_res_nm = rc1.res_area to dtl.alt_res_nm = rc1.res_nm  added by VS 12/20/17
                 and rc1.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc1.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 and dtl.res_area = rc.res_area
                 AND ro.siteid = rc1.site_num
                 AND odsopres.routingid = ro.routingid
                 AND odsopres.operationseq = ro.operationseq
                 AND odsopres.siteid = ro.siteid
                 AND odsopres.addresourcename = dtl.alt_res_prim_res_nm
                 AND odsopres.enterprise = ro.enterprise
              --AND odsopres.engine_id = eer.engine_id  --commented by sa on 12/21
              );
  
    -- For alt_res_flg = 'Y' alternate HW_SET_ID's
  
    INSERT INTO OPRESOURCEALT_STG
      (ALTRESOURCENAME, -- get alternate resources ie. tester,handler,hw_set_id
       ALTRESOURCESITEID, -- 'RES_SITE'
       ALTWORKCENTERNAME, -- get alternate resources ie. tester,handler,hw_set_id
       PRODRATE, -- -- get alternate resources ie. tester,handler,hw_set_id uph
       ENGINE_ID,
       ENTERPRISE,
       OPERATION,
       OPERATIONSEQ,
       ROUTINGID,
       SITEID,
       RESOURCENAME,
       WORKCENTERNAME,
       sourcedate)
      SELECT *
        FROM (SELECT -- dtl.alt_res_resource_nm || '_' || ro.siteid ALTRESOURCENAME, -- get alternate resources ie. tester,handler,hw_set_id
              /* dtl.alt_res_resource_nm || '_' || ldr.list_val_char ||
              ro.siteid ALTRESOURCENAME,*/
              DISTINCT rc1.eng_res_nm ALTRESOURCENAME,
                       'RES_SITE' ALTRESOURCESITEID, -- 'RES_SITE'
                       rc1.eng_res_nm ALTWORKCENTERNAME,
                       dtl.alt_res_uph,
                       eer.engine_id,
                       eer.enterprise,
                       ro.operation,
                       ro.operationseq,
                       ro.routingid,
                       ro.siteid, -- dtl.*,
                       odsopres.addresourcename RESOURCENAME,
                       odsopres.addworkcentername WORKCENTERNAME,
                       --  dtl.alt_res_prim_resource_nm,
                       sysdate
                FROM RES_RTE_ASN_SRC rc,
                     routingoperation_stg ro,
                     scmdata.RES_RTE_ASN_DTL_SRC dtl,
                     (select distinct eng_rte_id,
                                      prod_rte_seq_id,
                                      enterprise, --added by SA on 1/08
                                      prim_res_set_id,
                                      res_area
                        from RES_PART_ASN_SRC) pra,
                     ref_data.list_dtl_ref ldr,
                     opresourceadd_stg odsopres,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct res_nm,
                                      site_num,
                                      eng_res_nm,
                                      enterprise, --added by SA on 1/08
                                      res_area
                        from RES_CAP_SRC) rc1
               WHERE rc.alt_res_flg = 'Y'
                    --and   ro.enterprise    = rc.enterprise
                 AND ro.routingid = rc.eng_rte_id
                 AND ro.operationseq = rc.eng_oper_seq
                 AND ro.siteid = rc.eng_site_num
                 AND eer.enterprise = ro.enterprise
                 AND dtl.enterprise = rc.enterprise
                 AND rc1.enterprise = rc.enterprise --added by sa on 1/8
                 AND dtl.enterprise = pra.enterprise --added by sa on 1/8
                 and CASE
                       WHEN i_enterprise_nm LIKE 'OP%' THEN
                        'SCP_DAILY'
                       ELSE
                        eer.enterprise
                     END = dtl.enterprise
                 AND eer.enterprise = i_enterprise_nm
                 AND rc.eng_rte_id = dtl.eng_rte_id
                 AND rc.eng_oper_seq = dtl.eng_oper_seq
                 AND dtl.alt_res_type like 'HW_SET_ID%'
                    --and ro.routingid not like 'MAX1480AEPI+_S_4900%' --- should remove this later
                 AND nvl(dtl.alt_res_nm, 'dflt') != dtl.alt_res_prim_res_nm ---adding nvl to dtl.alt_res_nm to void nulls Added by VS 12/20/17
                 AND dtl.alt_res_flg = 'Y'
                 AND dtl.alt_res_nm = rc1.res_nm
                 AND rc1.res_area = pra.res_area
                 and rc1.res_area not in
                     ('NON ATE FT', 'DIECOAT', 'PKG', 'BUMP')
                 and rc1.res_area = pra.res_area
                 and pra.res_area = dtl.res_area
                 and dtl.res_area = rc.res_area
                 AND ro.siteid = rc1.site_num
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND pra.res_area = ldr.list_dtl_num
                 AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
                 AND pra.prim_res_set_id = dtl.prim_res_set_id
                 AND dtl.alt_res_prim_res_nm || '_' || ldr.list_val_char ||
                     ro.siteid = odsopres.addresourcename
                 AND odsopres.resourcename not like 'BLIN%'
                 AND odsopres.routingid = ro.routingid
                 AND odsopres.operationseq = ro.operationseq
                 AND odsopres.siteid = ro.siteid
                 AND odsopres.enterprise = ro.enterprise
              --AND odsopres.engine_id = ro.engine_id   --commented by sa on 12/21
              );
  
    -- Adding data for Hardware Alternates in RES_HW_ALT_SRC
  
    INSERT INTO OPRESOURCEALT_STG
      (ALTRESOURCENAME,
       ALTRESOURCESITEID, -- 'RES_SITE'
       ALTWORKCENTERNAME, -- get alternate resources ie. tester,handler,hw_set_id
       PRODRATE, -- -- get alternate resources ie. tester,handler,hw_set_id uph
       ENGINE_ID,
       ENTERPRISE,
       OPERATION,
       OPERATIONSEQ,
       ROUTINGID,
       SITEID,
       RESOURCENAME,
       WORKCENTERNAME,
       -- RESOURCECONFIGURATIONID,
       sourcedate)
      SELECT DISTINCT ALTRESOURCENAME, -- get alternate resources ie. tester,handler,hw_set_id
                      ALTRESOURCESITEID, -- 'RES_SITE'
                      ALTWORKCENTERNAME, -- get alternate resources ie. tester,handler,hw_set_id
                      PRODRATE, -- -- get alternate resources ie. tester,handler,hw_set_id uph
                      ENGINE_ID,
                      ENTERPRISE,
                      OPERATION,
                      OPERATIONSEQ,
                      ROUTINGID,
                      SITEID,
                      RESOURCENAME,
                      WORKCENTERNAME,
                      sysdate
        FROM (select ALTRESOURCENAME, -- get alternate resources ie. tester,handler,hw_set_id
                     ALTRESOURCESITEID, -- 'RES_SITE'
                     ALTWORKCENTERNAME, -- get alternate resources ie. tester,handler,hw_set_id
                     PRODRATE, -- -- get alternate resources ie. tester,handler,hw_set_id uph
                     ENGINE_ID,
                     ENTERPRISE,
                     OPERATION,
                     OPERATIONSEQ,
                     ROUTINGID,
                     SITEID,
                     RESOURCENAME,
                     WORKCENTERNAME
                FROM (SELECT ALT_HW_NM ||
                             substr(stg.ADDRESOURCENAME,
                                    instr(stg.ADDRESOURCENAME, '_', 1)) ALTRESOURCENAME,
                             stg.ADDRESOURCESITEID ALTRESOURCESITEID,
                             ALT_HW_NM ||
                             substr(stg.ADDRESOURCENAME,
                                    instr(stg.ADDRESOURCENAME, '_', 1)) ALTWORKCENTERNAME,
                             stg.PRODRATE,
                             stg.ENGINE_ID,
                             stg.enterprise,
                             stg.OPERATION,
                             stg.OPERATIONSEQ,
                             stg.ROUTINGID,
                             stg.SITEID,
                             stg.ADDRESOURCENAME AS RESOURCENAME,
                             stg.addworkcentername AS WORKCENTERNAME
                        FROM OPRESOURCEADD_stg           stg,
                             RES_HW_ALT_SRC              hsrc,
                             ref_data.eng_enterprise_ref eer
                       WHERE hsrc.PRIM_HW_NM =
                             substr(stg.ADDRESOURCENAME,
                                    1,
                                    instr(stg.ADDRESOURCENAME, '_', 1) - 1)
                         AND hsrc.eng_rte_id = stg.routingid
                         AND hsrc.ENG_OPER_SEQ = stg.operationseq
                         AND stg.enterprise = eer.enterprise
                         and CASE
                               WHEN i_enterprise_nm LIKE 'OP%' THEN
                                'SCP_DAILY'
                               ELSE
                                eer.enterprise
                             END = hsrc.enterprise
                            --      AND stg.engine_id = eer.engine_id
                         AND eer.enterprise = i_enterprise_nm)
              MINUS (select ALTRESOURCENAME, -- get alternate resources ie. tester,handler,hw_set_id
                           ALTRESOURCESITEID, -- 'RES_SITE'
                           ALTWORKCENTERNAME, -- get alternate resources ie. tester,handler,hw_set_id
                           PRODRATE, -- -- get alternate resources ie. tester,handler,hw_set_id uph
                           ENGINE_ID,
                           ENTERPRISE,
                           OPERATION,
                           OPERATIONSEQ,
                           ROUTINGID,
                           SITEID,
                           RESOURCENAME,
                           WORKCENTERNAME
                      FROM OPRESOURCEALT_STG
                     WHERE enterprise = i_enterprise_nm));
  
    -----below logic is for NON ATE----
    --populate data nonate data into opresourcealt_stg------
    -- For alt_res_flg = 'Y' case BIOV
  
    INSERT INTO OPRESOURCEALT_STG
      (ALTRESOURCENAME, -- get alternate resources ie. tester,handler,hw_set_id
       ALTRESOURCESITEID, -- 'RES_SITE'
       ALTWORKCENTERNAME, -- get alternate resources ie. tester,handler,hw_set_id
       PRODRATE, -- -- get alternate resources ie. tester,handler,hw_set_id uph
       ENGINE_ID,
       ENTERPRISE,
       OPERATION,
       OPERATIONSEQ,
       ROUTINGID,
       SITEID,
       RESOURCENAME,
       WORKCENTERNAME,
       sourcedate)
      SELECT distinct rc1.eng_res_nm ALTRESOURCENAME,
                      'RES_SITE' ALTRESOURCESITEID, -- 'RES_SITE'
                      rc1.eng_res_nm ALTWORKCENTERNAME,
                      dtl.alt_res_uph,
                      eer.engine_id,
                      eer.enterprise,
                      ro.operation,
                      ro.operationseq,
                      ro.routingid,
                      ro.siteid,
                      odsopres.resourcename RESOURCENAME,
                      odsopres.workcentername WORKCENTERNAME,
                      sysdate
        FROM RES_RTE_ASN_SRC rc,
             routingoperation_stg ro,
             scmdata.RES_RTE_ASN_DTL_SRC dtl,
             (select distinct eng_rte_id,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area,
                              enterprise
                from RES_PART_ASN_SRC
               where res_area = 'NON ATE FT') pra,
             ref_data.list_dtl_ref ldr,
             opresource_stg odsopres,
             ref_data.eng_enterprise_ref eer,
             (select distinct res_nm,
                              site_num,
                              eng_res_nm,
                              res_area,
                              enterprise --added by SA on 1/08
                from RES_CAP_SRC
               where enterprise like i_enterprise_nm
                 and res_area like 'NON%') rc1,
             res_nonate_attr_src rna
       WHERE rc.alt_res_flg = 'Y'
         AND ro.routingid = rc.eng_rte_id
         AND ro.operationseq = rc.eng_oper_seq
         AND ro.siteid = rc.eng_site_num
         AND eer.enterprise = ro.enterprise
         and dtl.enterprise = rc.enterprise
         and rc1.enterprise = rc.enterprise
         and CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = dtl.enterprise
         AND eer.enterprise = i_enterprise_nm
         AND odsopres.enterprise = ro.enterprise
            --and ro.enterprise = dtl.enterprise
         and dtl.enterprise = pra.enterprise
         and rna.enterprise = dtl.enterprise
         AND rc.eng_rte_id = dtl.eng_rte_id
         AND rc.eng_oper_seq = dtl.eng_oper_seq
         AND dtl.alt_res_type = 'BIOV'
         and dtl.alt_res_type = rna.res_type
         AND nvl(dtl.alt_res_nm, 1) != dtl.alt_res_prim_res_nm
         AND dtl.alt_res_flg = 'Y'
         AND dtl.res_area = pra.res_area
         and rc.res_area = dtl.res_area
         AND rc1.res_area like '%' || pra.res_area || '%'
         AND ro.siteid = rc1.site_num
         and rna.res_nm = rc1.res_nm
            --and rna.res_area = pra.res_area
         AND pra.prim_res_set_id = dtl.prim_res_set_id
         and pra.prim_res_set_id = rna.prim_res_set_id
         and pra.prod_rte_seq_id = rna.prod_rte_seq_id
         AND pra.res_area = ldr.list_dtl_num
         AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND pra.prim_res_set_id = dtl.prim_res_set_id
            --  AND odsopres.engine_id = eer.engine_id   --commented by sa on 12/21
         AND odsopres.siteid = ro.siteid
         AND odsopres.routingid = ro.routingid
         AND odsopres.operationseq = ro.operationseq
      union all
      --   For alt_res_flg = 'Y' case BIBR
      SELECT DISTINCT rc1.eng_res_nm ALTRESOURCENAME,
                      'RES_SITE' ALTRESOURCESITEID, -- 'RES_SITE'
                      rc1.eng_res_nm ALTWORKCENTERNAME,
                      dtl.alt_res_uph,
                      eer.engine_id,
                      eer.enterprise,
                      ro.operation,
                      ro.operationseq,
                      ro.routingid,
                      ro.siteid, -- dtl.*,
                      odsopres.addresourcename RESOURCENAME,
                      odsopres.addworkcentername WORKCENTERNAME,
                      sysdate
        FROM RES_RTE_ASN_SRC rc,
             routingoperation_stg ro,
             scmdata.RES_RTE_ASN_DTL_SRC dtl,
             (select distinct eng_rte_id,
                              prod_rte_seq_id,
                              prim_res_set_id,
                              res_area,
                              enterprise
                from RES_PART_ASN_SRC
               where res_area = 'NON ATE FT') pra,
             ref_data.list_dtl_ref ldr,
             opresourceadd_stg odsopres,
             ref_data.eng_enterprise_ref eer,
             (select distinct res_nm,
                              site_num,
                              eng_res_nm,
                              res_area,
                              enterprise
                from RES_CAP_SRC
               where enterprise like i_enterprise_nm
                 and res_area like 'NON%') rc1,
             res_nonate_attr_src rna
       WHERE rc.alt_res_flg = 'Y'
         AND ro.routingid = rc.eng_rte_id
         AND ro.operationseq = rc.eng_oper_seq
         AND ro.siteid = rc.eng_site_num
         AND eer.enterprise = ro.enterprise
         AND dtl.enterprise = rc.enterprise
         AND rc.enterprise = rc1.enterprise
         and CASE
               WHEN i_enterprise_nm LIKE 'OP%' THEN
                'SCP_DAILY'
               ELSE
                eer.enterprise
             END = dtl.enterprise
         AND eer.enterprise = i_enterprise_nm
         and dtl.enterprise = pra.enterprise
         and rna.enterprise = dtl.enterprise
         and odsopres.enterprise = ro.enterprise
         AND rc.eng_rte_id = dtl.eng_rte_id
         AND rc.eng_oper_seq = dtl.eng_oper_seq
         AND dtl.alt_res_type = 'BIBR'
         and dtl.alt_res_type = rna.res_type
         AND nvl(dtl.alt_res_nm, 1) != dtl.alt_res_prim_res_nm
         AND dtl.alt_res_flg = 'Y'
         AND dtl.res_area = pra.res_area
         and rc.res_area = dtl.res_area
         AND rc1.res_area like '%' || pra.res_area || '%'
         AND ro.siteid = rc1.site_num
         and rna.res_nm = rc1.res_nm
            --and rna.res_area = pra.res_area
            
         AND pra.prim_res_set_id = dtl.prim_res_set_id
         and pra.prim_res_set_id = rna.prim_res_set_id
         and pra.prod_rte_seq_id = rna.prod_rte_seq_id
         AND pra.res_area = ldr.list_dtl_num
         AND pra.prod_rte_seq_id = dtl.prod_rte_seq_id
         AND pra.prim_res_set_id = dtl.prim_res_set_id
            -- and ro.enterprise = dtl.enterprise
            
            --  AND odsopres.engine_id = eer.engine_id
         AND odsopres.siteid = ro.siteid
         AND odsopres.routingid = ro.routingid
         AND odsopres.operationseq = ro.operationseq;
  
    Commit; /* remove later  */
    frmdata.logs.info('OPRESOURCEALT_STG load from SRC to ODS is completed');
  
  END load_OPRESOURCEALT;

----------------------------------------------------------------------------------------------------------

/*--************************************************************
  --Procedure to load data into OPRESOURCE table --7 -- ALT_RTE_FLG= 'Y'
  --************************************************************/
/*PROCEDURE load_OPRESOURCE_ALTRTE(i_enterprise_nm IN VARCHAR2) IS

    -- lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date

  BEGIN

    frmdata.logs.begin_log('Start OPRESOURCE load from SRC to ODS');
    -- frmdata.delete_data('scmdata.OPRESOURCE_STG_1', NULL, NULL);

    -- add resources TESTER for those alt routes

    --   lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm);

    INSERT INTO OPRESOURCE_STG
      (ENTERPRISE,
       ROUTINGID,
       OPERATIONSEQ,
       SITEID,
       WORKCENTERNAME,
       RESOURCENAME,
       ENGINE_ID,
       sourcedate,
       RESOURCESITEID,
       OPERATION,
       PRODRATE)
      SELECT *
        FROM (SELECT eer.enterprise,
                      oper.routingid,
                      oper.operationseq,
                      oper.siteid,
                      rc.engine_res_name WORKCENTERNAME,
                      rc.engine_res_name RESOURCENAME,
                      eer.engine_id,
                      sysdate,
                      'RES_SITE',
                      oper.operation,
                      attr.uph
                 FROM RoutingOperation_STG oper,
                      RES_RTE_ALT_SRC alt,
                      RES_RTE_ASN_DTL_SRC dtl,
                      RES_ATTR_SRC attr,
                      (select distinct eng_routing,
                                       prod_route_seq_id,
                                       prim_resource_set_id,
                                       resource_area
                         from RES_PART_ASN_SRC) pra,
                      ref_data.list_dtl_ref ldr,
                      ref_data.eng_enterprise_ref eer,
                      (select distinct resource_name,
                                       site_id,
                                       engine_res_name,
                                       resource_area
                         from RES_CAP_SRC) rc
                WHERE -- oper.routingid =  'HS45Z_S1_2_4905_1_ALT_7' and
                oper.routingid = alt.ENG_ALT_RTE_ID
               --and   alt.ENG_ORIG_RTE_ID   = 'HS45Z_S1_2_4905_1'
             AND oper.operationseq = alt.ENG_OPER_SEQ
             AND alt.ENG_ORIG_RTE_ID = dtl.eng_rte_id
             AND alt.res_set_id = dtl.alt_rte_res_set_id
             AND alt.ENG_OPER_SEQ = dtl.eng_oper_seq
             AND dtl.prod_route_seq_id = attr.prod_route_seq_id
             AND dtl.prim_resource_set_id = attr.prim_resource_set_id
             AND dtl.alt_rte_res_set_id = attr.resource_set_id
             AND pra.resource_area = ldr.list_dtl_num
             AND pra.prod_route_seq_id = dtl.prod_route_seq_id
             AND pra.prim_resource_set_id = dtl.prim_resource_set_id
             AND attr.resource_type = 'TESTER'
             AND attr.resource_nm not like 'BLIN%'
             AND attr.resource_nm = rc.resource_name
             AND rc.resource_area = pra.resource_area
             AND oper.siteid = rc.site_id
             AND oper.enterprise = dtl.enterprise
             AND alt.enterprise = dtl.enterprise
             AND eer.enterprise = dtl.enterprise
             AND eer.enterprise = i_enterprise_nm);

    Commit; /* remove later  */
-- frmdata.logs.info('OPRESOURCE_STG load from SRC to ODS is completed');

--END load_OPRESOURCE_ALTRTE;

--************************************************************
--Procedure to load data into OPRESOURCEADD table --8 -- ALT_RTE_FLG= 'Y'
--************************************************************
-- PROCEDURE load_OPRESOURCEADD_ALTRTE(i_enterprise_nm IN VARCHAR2) IS

--  lv_cur_date DATE := scmdata.engines.get_plan_currentdate(i_enterprise_nm); --to get current date

/*  BEGIN

    frmdata.logs.begin_log('Start OPRESOURCEADD load from SRC to ODS');
    -- frmdata.delete_data('scmdata.OPRESOURCEADD_STG', NULL, NULL);

    --   lv_cur_date := scmdata.engines.get_plan_currentdate(i_enterprise_nm);

    -- add simultaneous resources HANDLERS

    INSERT INTO OPRESOURCEADD_STG
      (ADDRESOURCENAME,
       ADDRESOURCESITEID,
       ADDWORKCENTERNAME,
       ROUTINGID,
       SITEID,
       ENTERPRISE,
       ENGINE_ID,
       OPERATION,
       OPERATIONSEQ,
       PRODRATE,
       RESOURCENAME,
       WORKCENTERNAME,
       sourcedate)
      select *
        from (SELECT rc.engine_res_name ADDRESOURCENAME, -- 529 out of 574 expected rows
                     'RES_SITE', --  -- hardcode as 'RES_SITE'
                     rc.engine_res_name ADDWORKCENTERNAME,
                     oper.routingid,
                     oper.siteid,
                     eer.enterprise,
                     eer.engine_id,
                     oper.operation,
                     oper.operationseq,
                     attr.uph,
                     (select attr.resource_nm || '_' || ldr.list_val_char ||
                             oper1.siteid
                        from RoutingOperation_STG oper1,
                             RES_RTE_ALT_SRC alt1,
                             RES_RTE_ASN_DTL_SRC dtl,
                             RES_ATTR_SRC attr,
                             (select distinct eng_routing,
                                              prod_route_seq_id,
                                              prim_resource_set_id,
                                              resource_area
                                from RES_PART_ASN_SRC) pra,
                             ref_data.list_dtl_ref ldr
                       where oper1.routingid = alt1.ENG_ALT_RTE_ID
                         and oper1.operationseq = alt1.ENG_OPER_SEQ
                         and alt1.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                         and alt1.res_set_id = dtl.alt_rte_res_set_id
                         and alt1.ENG_OPER_SEQ = dtl.eng_oper_seq
                         and dtl.prod_route_seq_id = attr.prod_route_seq_id
                         AND pra.resource_area = ldr.list_dtl_num
                         AND pra.prod_route_seq_id = dtl.prod_route_seq_id
                         AND pra.prim_resource_set_id =
                             dtl.prim_resource_set_id
                         and dtl.prim_resource_set_id =
                             attr.prim_resource_set_id
                         and dtl.alt_rte_res_set_id = attr.resource_set_id
                         and attr.resource_type = 'TESTER'
                         AND attr.resource_nm not like 'BLIN%'
                            --and     alt1.ENG_ORIG_RTE_ID      = 'HS45Z_S1_2_4905_1'
                         and alt1.ENG_ALT_RTE_ID = oper.routingid
                         and oper1.operationseq = oper.operationseq) RESOURCENAME,
                     (select attr.resource_nm || '_' || ldr.list_val_char ||
                             oper1.siteid
                        from RoutingOperation_STG oper1,
                             RES_RTE_ALT_SRC alt1,
                             RES_RTE_ASN_DTL_SRC dtl,
                             RES_ATTR_SRC attr,
                             (select distinct eng_routing,
                                              prod_route_seq_id,
                                              prim_resource_set_id,
                                              resource_area
                                from RES_PART_ASN_SRC) pra,
                             ref_data.list_dtl_ref ldr
                       where oper1.routingid = alt1.ENG_ALT_RTE_ID
                         and oper1.operationseq = alt1.ENG_OPER_SEQ
                         and alt1.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                         and alt1.res_set_id = dtl.alt_rte_res_set_id
                         and alt1.ENG_OPER_SEQ = dtl.eng_oper_seq
                         and dtl.prod_route_seq_id = attr.prod_route_seq_id
                         AND pra.resource_area = ldr.list_dtl_num
                         AND pra.prod_route_seq_id = dtl.prod_route_seq_id
                         AND pra.prim_resource_set_id =
                             dtl.prim_resource_set_id
                         and dtl.prim_resource_set_id =
                             attr.prim_resource_set_id
                         and dtl.alt_rte_res_set_id = attr.resource_set_id
                         and attr.resource_type = 'TESTER'
                         AND attr.resource_nm not like 'BLIN%'
                            --and     alt1.ENG_ORIG_RTE_ID      = 'HS45Z_S1_2_4905_1'
                         and alt1.ENG_ALT_RTE_ID = oper.routingid
                         and oper1.operationseq = oper.operationseq) WORKCENTERNAME,
                     sysdate
                FROM RoutingOperation_STG oper,
                     RES_RTE_ALT_SRC alt,
                     RES_RTE_ASN_DTL_SRC dtl,
                     RES_ATTR_SRC attr,
                     (select distinct eng_routing,
                                      prod_route_seq_id,
                                      prim_resource_set_id,
                                      resource_area
                        from RES_PART_ASN_SRC) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct resource_name,
                                      site_id,
                                      engine_res_name,
                                      resource_area
                        from RES_CAP_SRC) rc
               WHERE oper.routingid = alt.ENG_ALT_RTE_ID
                    --and    alt.ENG_ORIG_RTE_ID   = 'HS45Z_S1_2_4905_1'
                 AND oper.operationseq = alt.ENG_OPER_SEQ
                 AND oper.operationseq = alt.ENG_OPER_SEQ
                 AND alt.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                 AND alt.res_set_id = dtl.alt_rte_res_set_id
                 AND alt.ENG_OPER_SEQ = dtl.eng_oper_seq
                 AND dtl.prod_route_seq_id = attr.prod_route_seq_id
                 AND dtl.prim_resource_set_id = attr.prim_resource_set_id
                 AND pra.resource_area = ldr.list_dtl_num
                 AND pra.prod_route_seq_id = dtl.prod_route_seq_id
                 AND pra.prim_resource_set_id = dtl.prim_resource_set_id
                 AND dtl.alt_rte_res_set_id = attr.resource_set_id
                    --  AND attr.resource_type = 'HW_SET_ID'
                    --  AND attr.hardware_name IS NOT NULL
                 AND attr.resource_type = 'HANDLER'
                 AND attr.resource_nm IS NOT NULL
                 AND attr.resource_nm = rc.resource_name
                 AND rc.resource_area = pra.resource_area
                 AND oper.siteid = rc.site_id
                 AND oper.enterprise = dtl.enterprise
                 AND alt.enterprise = dtl.enterprise
                 AND eer.enterprise = dtl.enterprise
                 AND eer.enterprise = i_enterprise_nm);
    -- Simultaneous resource HW_SET_ID's

    INSERT INTO OPRESOURCEADD_STG
      (ADDRESOURCENAME,
       ADDRESOURCESITEID,
       ADDWORKCENTERNAME,
       ROUTINGID,
       SITEID,
       ENTERPRISE,
       ENGINE_ID,
       OPERATION,
       OPERATIONSEQ,
       PRODRATE,
       RESOURCENAME,
       WORKCENTERNAME,
       sourcedate)
      select *
        from (SELECT rc.engine_res_name ADDRESOURCENAME, -- 529 out of 574 expected rows
                     'RES_SITE', --  -- hardcode as 'RES_SITE'
                     rc.engine_res_name ADDWORKCENTERNAME,
                     oper.routingid,
                     oper.siteid,
                     eer.enterprise,
                     eer.engine_id,
                     oper.operation,
                     oper.operationseq,
                     attr.uph,
                     (select attr.resource_nm || '_' || ldr.list_val_char ||
                             oper1.siteid
                        FROM RoutingOperation_STG oper1,
                             RES_RTE_ALT_SRC alt1,
                             RES_RTE_ASN_DTL_SRC dtl,
                             RES_ATTR_SRC attr,
                             (select distinct eng_routing,
                                              prod_route_seq_id,
                                              prim_resource_set_id,
                                              resource_area
                                from RES_PART_ASN_SRC) pra,
                             ref_data.list_dtl_ref ldr
                       WHERE oper1.routingid = alt1.ENG_ALT_RTE_ID
                         and oper1.operationseq = alt1.ENG_OPER_SEQ
                         and alt1.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                         and alt1.res_set_id = dtl.alt_rte_res_set_id
                         and alt1.ENG_OPER_SEQ = dtl.eng_oper_seq
                         and dtl.prod_route_seq_id = attr.prod_route_seq_id
                         AND pra.resource_area = ldr.list_dtl_num
                         AND pra.prod_route_seq_id = dtl.prod_route_seq_id
                         AND pra.prim_resource_set_id =
                             dtl.prim_resource_set_id
                         and dtl.prim_resource_set_id =
                             attr.prim_resource_set_id
                         and dtl.alt_rte_res_set_id = attr.resource_set_id
                         and attr.resource_type = 'TESTER'
                         AND attr.resource_nm not like 'BLIN%'
                            --and     alt1.ENG_ORIG_RTE_ID      = 'HS45Z_S1_2_4905_1'
                         and alt1.ENG_ALT_RTE_ID = oper.routingid
                         and oper1.operationseq = oper.operationseq) RESOURCENAME,
                     (select attr.resource_nm || '_' || ldr.list_val_char ||
                             oper1.siteid
                        from RoutingOperation_STG oper1,
                             RES_RTE_ALT_SRC alt1,
                             RES_RTE_ASN_DTL_SRC dtl,
                             RES_ATTR_SRC attr,
                             (select distinct eng_routing,
                                              prod_route_seq_id,
                                              prim_resource_set_id,
                                              resource_area
                                from RES_PART_ASN_SRC) pra,
                             ref_data.list_dtl_ref ldr
                       WHERE oper1.routingid = alt1.ENG_ALT_RTE_ID
                         AND oper1.operationseq = alt1.ENG_OPER_SEQ
                         AND alt1.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                         AND alt1.res_set_id = dtl.alt_rte_res_set_id
                         AND alt1.ENG_OPER_SEQ = dtl.eng_oper_seq
                         AND dtl.prod_route_seq_id = attr.prod_route_seq_id
                         AND pra.resource_area = ldr.list_dtl_num
                         AND pra.prod_route_seq_id = dtl.prod_route_seq_id
                         AND pra.prim_resource_set_id =
                             dtl.prim_resource_set_id
                         AND dtl.prim_resource_set_id =
                             attr.prim_resource_set_id
                         AND dtl.alt_rte_res_set_id = attr.resource_set_id
                         AND attr.resource_type = 'TESTER'
                         AND attr.resource_nm not like 'BLIN%'
                            --and     alt1.ENG_ORIG_RTE_ID      = 'HS45Z_S1_2_4905_1'
                         AND alt1.ENG_ALT_RTE_ID = oper.routingid
                         AND oper1.operationseq = oper.operationseq) WORKCENTERNAME,
                     sysdate
                FROM RoutingOperation_STG oper,
                     RES_RTE_ALT_SRC alt,
                     RES_RTE_ASN_DTL_SRC dtl,
                     RES_ATTR_SRC attr,
                     (select distinct eng_routing,
                                      prod_route_seq_id,
                                      prim_resource_set_id,
                                      resource_area
                        from RES_PART_ASN_SRC) pra,
                     ref_data.list_dtl_ref ldr,
                     ref_data.eng_enterprise_ref eer,
                     (select distinct resource_name,
                                      site_id,
                                      engine_res_name,
                                      resource_area
                        from RES_CAP_SRC) rc
               WHERE oper.routingid = alt.ENG_ALT_RTE_ID
                    --and    alt.ENG_ORIG_RTE_ID   = 'HS45Z_S1_2_4905_1'
                 AND oper.operationseq = alt.ENG_OPER_SEQ
                 AND oper.operationseq = alt.ENG_OPER_SEQ
                 AND alt.ENG_ORIG_RTE_ID = dtl.eng_rte_id
                 AND attr.hardware_name = rc.resource_name
                 AND rc.resource_area = pra.resource_area
                 AND oper.siteid = rc.site_id
                 AND alt.res_set_id = dtl.alt_rte_res_set_id
                 AND alt.ENG_OPER_SEQ = dtl.eng_oper_seq
                 AND dtl.prod_route_seq_id = attr.prod_route_seq_id
                 AND dtl.prim_resource_set_id = attr.prim_resource_set_id
                 AND pra.resource_area = ldr.list_dtl_num
                 AND pra.prod_route_seq_id = dtl.prod_route_seq_id
                 AND pra.prim_resource_set_id = dtl.prim_resource_set_id
                 AND dtl.alt_rte_res_set_id = attr.resource_set_id
                 AND attr.resource_type = 'HW_SET_ID'
                 AND attr.hardware_name IS NOT NULL
                    -- AND attr.resource_type = 'HANDLER'
                    --  AND attr.resource_nm IS NOT NULL
                 AND oper.enterprise = dtl.enterprise
                 AND alt.enterprise = dtl.enterprise
                 AND eer.enterprise = dtl.enterprise
                 AND eer.enterprise = i_enterprise_nm);


   commit;

  END load_OPRESOURCEADD_ALTRTE;*/

----------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------
END Resources_Load;
/
