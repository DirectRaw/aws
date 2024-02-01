SELECT json_object( 'ID' IS a.ID, 'ParentID' IS a.PARENTID, 'Name' IS a.NAME, 'Lvl' IS a.LVL, 'ParentLvl' IS a.PARENTLVL, 'ParentName' IS a.PARENTNAME, 'RootFlag' IS a.ROOTFLAG) JSON
FROM
  (SELECT l.dsend_id  AS ID,
    l.parnt_id        AS PARENTID,
    l.dsend_name      AS NAME,
    l.strct_root_flag AS ROOTFLAG,
    l.dsend_lvl       AS LVL,
    l.parnt_lvl       AS PARENTLVL,
    l.PARNT_NAME      AS PARENTNAME
  FROM CDWR.LEGAL_ENT_ASSOC_DNORM l
  WHERE l.strct_code = '101'
  AND l.net_lvl      = 1
  UNION
  SELECT l.dsend_id   AS ID,
    ''                AS PARENTID,
    l.dsend_name      AS NAME,
    l.strct_root_flag AS ROOTFLAG,
    l.dsend_lvl       AS LVL,
    l.parnt_lvl       AS PARENTLVL,
    l.parnt_name      AS PARENTNAME
  FROM CDWR.LEGAL_ENT_ASSOC_DNORM l
  WHERE l.strct_code = '101'
  AND l.dsend_lvl    = 1
  ) a
ORDER BY a.LVL,
  a.PARENTLVL