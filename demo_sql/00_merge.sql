CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:esquema);

DROP TABLE IF EXISTS IDENTIFIER(:tabla);

CREATE TABLE IDENTIFIER(:tabla) AS
SELECT * FROM VALUES
  (1, 'Ana',   'Madrid'),
  (2, 'Bruno', 'Sevilla'),
  (3, 'Carla', 'Bilbao')
AS t(id, nombre, ciudad);

SELECT * FROM IDENTIFIER(:tabla) ORDER BY id;
