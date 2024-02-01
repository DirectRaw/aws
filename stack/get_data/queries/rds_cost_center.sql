SELECT json_object( 'ID' IS a.ID, 'ParentID' IS a.PARENTID, 'Name' IS a.NAME, 'Lvl' IS a.LVL, 'ParentLvl' IS a.PARENTLVL, 'ParentName' IS a.PARENTNAME, 'RootFlag' IS a.ROOTFLAG, 'Status' IS a.STATUS, 'CompanyCode' IS a.COMPANYCODE, 'TDCIndicator' IS a.TDCINDICATOR, 'COUNTRY' IS a.COUNTRY) JSON
FROM
  (SELECT d.dsend_id                                            AS ID,
    d.parnt_id                                                  AS PARENTID,
    d.dsend_name                                                AS NAME,
    d.DSEND_lvl                                                 AS LVL,
    d.parnt_lvl                                                 AS PARENTLVL,
    d.STRCT_ROOT_FLAG                                           AS ROOTFLAG,
    d.parnt_name                                                AS PARENTNAME,
    o.ATTR_VAL                                                  AS COMPANYCODE,
    (SELECT DISTINCT
      CASE strct_code
        WHEN '686'
        THEN 'SRAP'
        WHEN '802'
        THEN 'TDC'
        WHEN '844'
        THEN 'MSA'
        ELSE 'NA'
      END AS TDC_Indicator
    FROM cdwr.org_assoc_dnorm
    WHERE dsend_id  = d.dsend_id
    AND STRCT_CODE IN ('686', '844', '802')
    ) AS TDCINDICATOR,
    (SELECT ATTR_VAL FROM CDWR.ORG_ATTR WHERE ORG_ID = d.dsend_id and ORG_ATTR_TYPE_ID = 7) AS COUNTRY,
    (SELECT STTUS_CODE FROM CDWR.ORG WHERE ORG_ID = d.dsend_id) AS STATUS,
    RANK() over (partition BY dsend_id order by parnt_lvl DESC) RANKNUM
  FROM CDWR.ORG_ASSOC_DNORM d
    LEFT OUTER JOIN CDWR.ORG_ATTR o
    ON d.dsend_id          = o.org_id
    AND o.ORG_ATTR_TYPE_ID = 2
  WHERE strct_code = '926'
  and net_lvl = 1
  AND parnt_id    <> dsend_id
UNION 
  SELECT d.dsend_id                                             AS ID,
    ''                                                          AS PARENTID,
    d.dsend_name                                                AS NAME,
    d.DSEND_lvl                                                 AS LVL,
    d.parnt_lvl                                                 AS PARENTLVL,
    d.STRCT_ROOT_FLAG                                           AS ROOTFLAG,
    d.parnt_name                                                AS PARENTNAME,
    o.ATTR_VAL                                                  AS COMPANYCODE,
    (SELECT DISTINCT
      CASE strct_code
        WHEN '686'
        THEN 'SRAP'
        WHEN '802'
        THEN 'TDC'
        WHEN '844'
        THEN 'MSA'
        ELSE 'NA'
      END AS TDCINDICATOR
    FROM cdwr.org_assoc_dnorm
    WHERE dsend_id  = d.dsend_id
    AND STRCT_CODE IN ('686', '844', '802')
    ) AS TDC_Indicator,
    (SELECT ATTR_VAL FROM CDWR.ORG_ATTR WHERE ORG_ID = d.dsend_id and ORG_ATTR_TYPE_ID = 7) AS COUNTRY,
    (SELECT STTUS_CODE FROM CDWR.ORG WHERE ORG_ID = d.dsend_id) AS STATUS,
    RANK() over (partition BY dsend_id order by parnt_lvl DESC) RANKNUM
  FROM CDWR.ORG_ASSOC_DNORM d
    LEFT OUTER JOIN CDWR.ORG_ATTR o
    ON d.dsend_id          = o.org_id
    AND o.ORG_ATTR_TYPE_ID = 2
  WHERE strct_code = '926'
  and dsend_lvl = 1
  ) a
WHERE a.RankNum = 1 and a.ROOTFLAG = 'Y' and a.ID not like 'Z%' and a.ID not like 'C%'
ORDER BY a.LVL, a.PARENTLVL
