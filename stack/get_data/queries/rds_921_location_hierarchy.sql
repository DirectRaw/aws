SELECT json_object( 'ID' IS a.ID, 'ParentID' IS a.PARENTID, 'Name' IS a.NAME, 'Lvl' IS a.LVL, 'ParentLvl' IS a.PARENTLVL, 'ParentName' IS a.PARENTNAME, 'RootFlag' IS a.ROOTFLAG, 'Status' IS a.STATUS) JSON
FROM
  (SELECT o.dsend_id                                            AS ID,
    o.parnt_id                                                  AS PARENTID,
    o.dsend_name                                                AS NAME,
    o.DSEND_lvl                                                 AS LVL,
    o.parnt_lvl                                                 AS PARENTLVL,
    o.STRCT_ROOT_FLAG                                           AS ROOTFLAG,
    o.parnt_name                                                AS PARENTNAME,
    c.STTUS_CODE                                                AS STATUS,
    RANK() OVER (partition BY dsend_id order by parnt_lvl DESC) AS RANKNUM
  FROM CDWR.SITE_ASSOC_DNORM o
  LEFT OUTER JOIN CDWR.SITE_ATTR s
  ON o.dsend_id           = s.site_id
  AND s.SITE_ATTR_TYPE_ID = 4
  LEFT OUTER JOIN CDWR.SITE c 
  ON o.dsend_id = c.site_id
  WHERE o.strct_code      = '921'
  AND o.PARNT_ID         <> o.DSEND_ID
  UNION
  SELECT o.dsend_id                                             AS ID,
    ''                                                         AS PARENTID,
    o.dsend_name                                                AS NAME,
    o.DSEND_lvl                                                 AS LVL,
    o.parnt_lvl                                                 AS PARENTLVL,
    o.STRCT_ROOT_FLAG                                           AS ROOTFLAG,
    o.parnt_name                                                AS PARENTNAME,
    c.STTUS_CODE                                                AS STATUS,
    RANK() OVER (partition BY dsend_id order by parnt_lvl DESC) AS RANKNUM
  FROM CDWR.SITE_ASSOC_DNORM o
  LEFT OUTER JOIN CDWR.SITE_ATTR s
  ON o.dsend_id           = s.site_id
  AND s.SITE_ATTR_TYPE_ID = 4
  LEFT OUTER JOIN CDWR.SITE c 
  ON o.dsend_id = c.site_id
  WHERE o.strct_code      = '921'
  AND o.DSEND_lvl         = 1
  ) a
WHERE a.RankNum = 1
ORDER BY a.LVL, a.PARENTLVL
