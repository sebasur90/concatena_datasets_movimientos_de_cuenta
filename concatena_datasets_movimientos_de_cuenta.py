import os
import pandas as pd
import sys
print(sys.argv)


ruta = input("Ingresar ruta del dataset: ")

datos = pd.read_csv(ruta, sep=";")
datos.columns = ['fecha', 'descripcion', 'combte', 'debito', 'credito', 'saldo',
                 'codigo']
datos.drop("codigo", axis=1, inplace=True)
datos.fecha = pd.to_datetime(datos.fecha, format='%Y%m%d')
datos.sort_values(by="fecha", ascending=True, inplace=True)
datos.reset_index(drop=True)

anos_ultimo_csv = list(datos.fecha.map(lambda x: x.year).unique())


dataset_anteriores = os.listdir(os.getcwd()+"/datasets")
anos_ya_guardados = [
    x for x in anos_ultimo_csv if f"data_{x}.csv" in dataset_anteriores]
anos_ya_guardados.sort()
ultimo_ano_guardado = anos_ya_guardados[-1]
ultimo_ano_guardado = f"{ultimo_ano_guardado}-12-31"
datos = datos[datos.fecha > ultimo_ano_guardado]
datos.fecha = pd.to_datetime(datos.fecha, format='%Y%m%d')
datos.sort_values(by="fecha", ascending=True, inplace=True)
datos.reset_index(drop=True, inplace=True)

anos_ultimo_csv = list(datos.fecha.map(lambda x: x.year).unique())
if len(anos_ultimo_csv) > 1:
    datos_a_guardar = datos[(datos.fecha > str(anos_ultimo_csv[0])) & (
        datos.fecha < str(anos_ultimo_csv[-1]))]
    datos_a_guardar.fecha = datos_a_guardar.fecha.map(
        lambda x: int(str(x.date()).replace("-", "")))
    datos = datos[(datos.fecha > str(anos_ultimo_csv[-1]))]
    path = os.getcwd()+"/datasets/"
    path = path.replace("\\", "/")
    datos_a_guardar.to_csv(
        f"{path}data_{anos_ultimo_csv[0]}.csv", index=False, sep=";")


datos.fecha = datos.fecha.map(lambda x: int(str(x.date()).replace("-", "")))
datos.columns = ['FECHA', 'DESCRIPCION',
                 'COMBTE', 'DEBITO', 'CREDITO', 'SALDO']

lista_datasets = []
for dataset in os.listdir(os.getcwd()+"/datasets"):
    dataset_cargado = pd.read_csv(
        os.getcwd()+f"/datasets/{dataset}", sep=";")
    dataset_cargado.columns = ['FECHA', 'DESCRIPCION',
                               'COMBTE', 'DEBITO', 'CREDITO', 'SALDO']
    lista_datasets.append(dataset_cargado)
anteriores = pd.concat(lista_datasets, axis=0, ignore_index=True)


anteriores_y_actual = [anteriores, datos]
total = pd.concat(anteriores_y_actual, axis=0, ignore_index=True)


total.sort_values(by="FECHA", ascending=False, inplace=True)
total.reset_index(drop=True, inplace=True)

ruta = f"{os.getcwd()}/data_{total.FECHA.iloc[-1]}_{total.FECHA.iloc[0]}.csv"
total.to_csv(ruta, sep=";", index=False)
