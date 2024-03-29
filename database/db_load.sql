INSERT INTO TB_COUNTRY(Nombre)
(SELECT DISTINCT TEMPORAL.TEMP_COUNTRY FROM TEMPORAL);

INSERT INTO TB_YEAR(Fecha)
(SELECT DISTINCT TEMPORAL.TEMP_YEAR FROM TEMPORAL);

INSERT INTO TSUNAMI(ID_Fecha, ID_Pais, Water_height, Total_deaths, Total_damage, Total_houses_destroyed, Total_houses_damaged)
SELECT
    (SELECT TOP 1 ID_Fecha FROM TB_YEAR WHERE TEMPORAL.TEMP_YEAR = TB_YEAR.Fecha),
    (SELECT TOP 1 ID_Pais FROM TB_COUNTRY WHERE TEMPORAL.TEMP_COUNTRY = TB_COUNTRY.Nombre),
    CAST(TEMPORAL.TEMP_WATER_HEIGHT AS FLOAT),
    TEMPORAL.TEMP_TOTAL_DEATHS,
    CAST(TEMPORAL.TEMP_TOTAL_DAMAGE AS FLOAT),
    TEMPORAL.TEMP_TOTAL_HOUSES_DESTROYED,
    TEMPORAL.TEMP_TOTAL_HOUSES_DAMAGED
FROM TEMPORAL;