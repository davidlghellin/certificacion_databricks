dbutils.widgets.text("usuario_prohibido","nadie")

prohibido = dbutils.widgets.get("usuario_prohibido")
usuario = dbutils.jobs.taskValues.get(
    taskKey="obtener_usuario", key="usuario", debugValue="test")

if usuario == prohibido:
    print("ERRRR")
    dbutils.jobs.taskValues.set(key="alerta",value=True)
else:
    print("OK")
    dbutils.jobs.taskValues.set(key="alerta",value=False)
