CREATE OR REPLACE TEMP VIEW cambios AS
SELECT * FROM VALUES
  (1, 'Ana',   :ciudad_nueva, 'UPDATE'),
  (2, NULL,    NULL,          'DELETE'),
  (4, 'Diego', 'Valencia',    'INSERT')
AS t(id, nombre, ciudad, op);

MERGE INTO IDENTIFIER(:tabla) t
USING cambios s
ON t.id = s.id
WHEN MATCHED AND s.op = 'DELETE' THEN
  DELETE
WHEN MATCHED AND s.op = 'UPDATE' THEN
  UPDATE SET t.ciudad = s.ciudad
WHEN NOT MATCHED AND s.op = 'INSERT' THEN
  INSERT (id, nombre, ciudad) VALUES (s.id, s.nombre, s.ciudad);

SELECT * FROM IDENTIFIER(:tabla) ORDER BY id;