CREATE DATABASE capacidad_covid_db;

CREATE TABLE capacidad_atencion_por_municipio(
    id_municipio    INT PRIMARY KEY,
    municipio   VARCHAR(150),
    camas            INT,
    camas_uci        INT
); 

INSERT INTO capacidad_atencion_por_municipio (id_municipio, municipio, camas, camas_uci) VALUES (1, 'Bogotá D.C.', 4500, 750), 
(2, 'Medellín', 2800, 420), 
(3, 'Cali', 2100, 315), 
(4, 'Barranquilla', 1800, 270), 
(5, 'Bucaramanga', 1200, 180), 
(6, 'Cartagena', 1500, 225), 
(7, 'Cúcuta', 900, 135), 
(8, 'Pereira', 850, 110), 
(9, 'Ibagué', 700, 95), 
(10, 'Manizales', 600, 80);