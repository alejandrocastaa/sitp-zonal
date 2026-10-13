-- q1_rollup: Roll-up demanda: operador x franja, subtotales y total
SELECT * FROM (
SELECT o.operador_nombre AS operador, t.franja AS franja, COUNT(*) AS validaciones
FROM fact_validaciones f JOIN dim_tiempo t ON t.tiempo_sk=f.tiempo_sk
JOIN dim_operador o ON o.operador_sk=f.operador_sk
GROUP BY o.operador_nombre, t.franja
UNION ALL
SELECT o.operador_nombre, 'TOTAL OPERADOR', COUNT(*)
FROM fact_validaciones f JOIN dim_operador o ON o.operador_sk=f.operador_sk
GROUP BY o.operador_nombre
UNION ALL
SELECT 'TOTAL GENERAL', '', COUNT(*) FROM fact_validaciones
) ORDER BY (operador='TOTAL GENERAL'), operador, (franja LIKE 'TOTAL%'), franja;

-- q1_drilldown: Drill-down: de franja a hora del dia
SELECT t.franja, t.hora, COUNT(*) AS validaciones,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (), 2) AS pct_total
FROM fact_validaciones f JOIN dim_tiempo t ON t.tiempo_sk=f.tiempo_sk
GROUP BY t.franja, t.hora ORDER BY t.hora;

-- q1_slice: Slice: solo dias habiles, pico vs valle
SELECT CASE WHEN t.hora IN (5,6,7,8) THEN 'Pico manana' WHEN t.hora IN (16,17,18,19) THEN 'Pico tarde' ELSE 'Valle' END AS tipo_hora,
       COUNT(*) AS validaciones,
       COUNT(DISTINCT t.tiempo_sk) AS horas_observadas,
       ROUND(1.0*COUNT(*)/COUNT(DISTINCT t.tiempo_sk), 0) AS validaciones_por_hora
FROM fact_validaciones f JOIN dim_tiempo t ON t.tiempo_sk=f.tiempo_sk
WHERE t.es_fin_de_semana = 0
GROUP BY 1 ORDER BY validaciones_por_hora DESC;

-- q1_dice: Dice: top operadores x franjas pico, 15 rutas con mas demanda
SELECT r.linea_nombre AS linea, r.ruta_nombre AS ruta, o.operador_nombre AS operador, COUNT(*) AS validaciones
FROM fact_validaciones f
JOIN dim_tiempo t ON t.tiempo_sk=f.tiempo_sk
JOIN dim_ruta r ON r.ruta_sk=f.ruta_sk
JOIN dim_operador o ON o.operador_sk=f.operador_sk
WHERE t.franja IN ('Mañana (5-8)','Tarde-noche (16-19)')
  AND o.operador_sk IN (SELECT operador_sk FROM fact_validaciones GROUP BY operador_sk ORDER BY COUNT(*) DESC LIMIT 3)
GROUP BY r.linea_nombre, r.ruta_nombre, o.operador_nombre
ORDER BY validaciones DESC LIMIT 15;

-- q1_pivot: Pivot: dia de la semana x franja
SELECT t.nombre_dia AS dia,
  SUM(CASE WHEN t.franja LIKE 'Madrugada%'  THEN 1 ELSE 0 END) AS madrugada,
  SUM(CASE WHEN t.franja LIKE 'Mañana%'     THEN 1 ELSE 0 END) AS manana,
  SUM(CASE WHEN t.franja LIKE 'Media%'      THEN 1 ELSE 0 END) AS media_manana,
  SUM(CASE WHEN t.franja LIKE 'Mediodía%'   THEN 1 ELSE 0 END) AS mediodia_tarde,
  SUM(CASE WHEN t.franja LIKE 'Tarde%'      THEN 1 ELSE 0 END) AS tarde_noche,
  SUM(CASE WHEN t.franja LIKE 'Noche%'      THEN 1 ELSE 0 END) AS noche,
  COUNT(*) AS total
FROM fact_validaciones f JOIN dim_tiempo t ON t.tiempo_sk=f.tiempo_sk
GROUP BY t.dia_semana, t.nombre_dia ORDER BY t.dia_semana;

-- q2_rollup: Roll-up recaudo: operador x tipo de perfil, subtotales y total
SELECT * FROM (
SELECT o.operador_nombre AS operador,
       CASE p.es_subsidiado WHEN 1 THEN 'Subsidiado' ELSE 'Tarifa plena' END AS tipo_perfil,
       COUNT(*) AS validaciones, SUM(f.valor) AS recaudo_nominal
FROM fact_validaciones f JOIN dim_operador o ON o.operador_sk=f.operador_sk
JOIN dim_perfil p ON p.perfil_sk=f.perfil_sk
GROUP BY o.operador_nombre, p.es_subsidiado
UNION ALL
SELECT o.operador_nombre, 'TOTAL OPERADOR', COUNT(*), SUM(f.valor)
FROM fact_validaciones f JOIN dim_operador o ON o.operador_sk=f.operador_sk
GROUP BY o.operador_nombre
UNION ALL
SELECT 'TOTAL GENERAL', '', COUNT(*), SUM(valor) FROM fact_validaciones
) ORDER BY (operador='TOTAL GENERAL'), operador, (tipo_perfil LIKE 'TOTAL%'), tipo_perfil;

-- q2_drilldown: Drill-down: de operador a ruta (recaudo y % subsidiado)
SELECT o.operador_nombre AS operador, r.ruta_nombre AS ruta,
       SUM(f.valor) AS recaudo_nominal,
       ROUND(100.0*SUM(CASE WHEN p.es_subsidiado=1 THEN f.valor ELSE 0 END)/NULLIF(SUM(f.valor),0), 1) AS pct_recaudo_subsidiado,
       ROUND(100.0*SUM(CASE WHEN p.es_subsidiado=1 THEN 1 ELSE 0 END)/COUNT(*), 1) AS pct_validaciones_subsidiadas
FROM fact_validaciones f
JOIN dim_operador o ON o.operador_sk=f.operador_sk
JOIN dim_ruta r ON r.ruta_sk=f.ruta_sk
JOIN dim_perfil p ON p.perfil_sk=f.perfil_sk
GROUP BY o.operador_nombre, r.ruta_nombre
ORDER BY recaudo_nominal DESC LIMIT 20;

-- q2_slice: Slice: solo perfiles subsidiados
SELECT p.perfil_nombre AS perfil, COUNT(*) AS validaciones, SUM(f.valor) AS recaudo_nominal,
       SUM(CASE WHEN f.valor=0 THEN 1 ELSE 0 END) AS validaciones_sin_cobro
FROM fact_validaciones f JOIN dim_perfil p ON p.perfil_sk=f.perfil_sk
WHERE p.es_subsidiado = 1
GROUP BY p.perfil_nombre ORDER BY validaciones DESC;

-- q2_pivot: Pivot: operador x (subsidiado / tarifa plena), recaudo y proporcion
SELECT o.operador_nombre AS operador,
  SUM(CASE WHEN p.es_subsidiado=1 THEN f.valor ELSE 0 END) AS recaudo_subsidiado,
  SUM(CASE WHEN p.es_subsidiado=0 THEN f.valor ELSE 0 END) AS recaudo_tarifa_plena,
  SUM(f.valor) AS recaudo_total,
  ROUND(100.0*SUM(CASE WHEN p.es_subsidiado=1 THEN f.valor ELSE 0 END)/NULLIF(SUM(f.valor),0), 2) AS pct_subsidiado
FROM fact_validaciones f JOIN dim_operador o ON o.operador_sk=f.operador_sk
JOIN dim_perfil p ON p.perfil_sk=f.perfil_sk
GROUP BY o.operador_nombre ORDER BY recaudo_total DESC;

-- q3_perfil_zona: Perfil x zona (operador): validaciones y % dentro de la zona
WITH base AS (
  SELECT o.operador_nombre AS zona, p.perfil_nombre AS perfil, p.es_subsidiado AS sub, COUNT(*) AS validaciones
  FROM fact_validaciones f JOIN dim_operador o ON o.operador_sk=f.operador_sk
  JOIN dim_perfil p ON p.perfil_sk=f.perfil_sk
  GROUP BY o.operador_nombre, p.perfil_nombre, p.es_subsidiado)
SELECT zona, perfil, sub AS es_subsidiado, validaciones,
       ROUND(100.0*validaciones/SUM(validaciones) OVER (PARTITION BY zona), 2) AS pct_en_zona
FROM base ORDER BY zona, validaciones DESC;

-- q3_dice: Dice: subsidiado vs tarifa plena en franjas clave
SELECT t.franja, CASE p.es_subsidiado WHEN 1 THEN 'Subsidiado' ELSE 'Tarifa plena' END AS tipo_perfil,
       COUNT(*) AS validaciones,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (PARTITION BY p.es_subsidiado), 2) AS pct_del_grupo
FROM fact_validaciones f JOIN dim_tiempo t ON t.tiempo_sk=f.tiempo_sk
JOIN dim_perfil p ON p.perfil_sk=f.perfil_sk
WHERE t.franja IN ('Mañana (5-8)','Mediodía-tarde (12-15)','Tarde-noche (16-19)')
GROUP BY t.franja, p.es_subsidiado ORDER BY t.franja, tipo_perfil;

-- q3_pivot: Pivot: perfil x tipo de dia (habil vs fin de semana)
SELECT p.perfil_nombre AS perfil,
  SUM(CASE WHEN t.es_fin_de_semana=0 THEN 1 ELSE 0 END) AS dias_habiles,
  SUM(CASE WHEN t.es_fin_de_semana=1 THEN 1 ELSE 0 END) AS fin_de_semana,
  ROUND(100.0*SUM(CASE WHEN t.es_fin_de_semana=1 THEN 1 ELSE 0 END)/COUNT(*), 1) AS pct_fin_de_semana,
  ROUND(100.0*SUM(CASE WHEN f.valor=0 THEN 1 ELSE 0 END)/COUNT(*), 1) AS pct_sin_cobro
FROM fact_validaciones f JOIN dim_tiempo t ON t.tiempo_sk=f.tiempo_sk
JOIN dim_perfil p ON p.perfil_sk=f.perfil_sk
GROUP BY p.perfil_nombre ORDER BY (dias_habiles+fin_de_semana) DESC;
