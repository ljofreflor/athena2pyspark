
def producto_nuevo(subclase, marca, lift):   
    return """
    WITH fuente_filtrada AS
    (
    with items_filtrado AS (
        SELECT DISTINCT item_hash.item_id
        FROM prod_jumbo.item_hash
        WHERE item_hash.item_subclass_cd = {0}
                OR item_hash.brand_cd = '{1}')
    SELECT fuente.party_id, fuente.tran_start_dt, fuente.item_id from fuente join items_filtrado
    on fuente.item_id = items_filtrado.item_id
      where cast(fuente.party_id as integer) > 0
    )
    
    -- select count(*) from fuente_filtrada 35 929 589
    
    ,
    subclasepop AS
    (
      SELECT fuente_filtrada.party_id,
             item_hash.item_subclass_cd,
             COUNT(DISTINCT (fuente_filtrada.tran_start_dt)) AS visitas
      FROM fuente_filtrada
        LEFT JOIN item_hash ON (fuente_filtrada.item_id = item_hash.item_id)
      GROUP BY fuente_filtrada.party_id,
               item_hash.item_subclass_cd
    )
    
    -- select count(*) from subclasepop 15867270
    
    ,
    marcapop AS
    (
      SELECT fuente_filtrada.party_id,
             item_hash.brand_cd,
             COUNT(DISTINCT (fuente_filtrada.tran_start_dt)) AS visitas
      FROM fuente_filtrada
        LEFT JOIN item_hash ON (fuente_filtrada.item_id = item_hash.item_id)
      GROUP BY fuente_filtrada.party_id,
               item_hash.brand_cd
    )
    
    -- select count(*) from marcapop 2336032
    ,
    pobsubclase AS
    (
      SELECT subclasepop.party_id,
             subclasepop.visitas / b.visitasprom AS lift_subclase
      FROM subclasepop
        LEFT JOIN (SELECT subclasepop.item_subclass_cd,
                          AVG(subclasepop.visitas) AS visitasprom
                   FROM subclasepop
                   GROUP BY 1) AS b ON subclasepop.item_subclass_cd = b.item_subclass_cd
      
      where subclasepop.item_subclass_cd={0}
    )
    -- select count(*) from pobsubclase 15867270
    
    ,
    pobmarca AS
    (
      -- metrica poblacionales marca
      SELECT marcapop.party_id,
             marcapop.visitas / b.visitasprom AS lift_marca
      FROM marcapop
        LEFT JOIN (SELECT marcapop.brand_cd,
                          AVG(marcapop.visitas) AS visitasprom
                   FROM marcapop
                   GROUP BY 1) AS b ON marcapop.brand_cd = b.brand_cd
      
      where marcapop.brand_cd='{1}'
      
    )
    
    ,
    lift
    as (
    SELECT pobsubclase.party_id,
           pobsubclase.lift_subclase,
           pobmarca.lift_marca,
           pobsubclase.lift_subclase*pobmarca.lift_marca AS lift_afinidad
    FROM pobsubclase
      JOIN pobmarca ON pobsubclase.party_id = pobmarca.party_id
      order by 4 desc)
    ,
    contact as
    (
      select party_id,  jumbo_cl_ind_email from clientes where ju_mail=1
      )
      
      ,
      listado as 
      (
      
    select a.*,b.jumbo_cl_ind_email from lift as a left join contact as b on a.party_id=cast(b.party_id as varchar) where b.party_id is not null
      )
      -- select count(*) from listado 108196
       	-- 76506
     -- select count(*) from lift  -- 304730
     ,
     list_form as (
     select party_id ,
             '300' as promo_id,
              1 as  comm_channel_cd,
             'PopCorn' as codigo_siebel,
              '300' as codigo_motor,
              1 as communication_id,
              1 as page_id,
              jumbo_cl_ind_email as datos_de_contacto,
              61009 as correlativo,
       CASE WHEN rand()<= 0.05 then 1 else 0 end grupo
              from listado where lift_afinidad>{2}
      )
      
      select count(*), sum(grupo) from list_form""".format(subclase, marca, lift)