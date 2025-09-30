import polars as pl
from polars import schema
import pandas as pd

comparativo = pd.read_excel("Anexo 9. Descripción de Servicios Contratados y Tarifas por Sede.xlsx", sheet_name="Comparativo")
municipios = pd.read_excel("Anexo 9. Descripción de Servicios Contratados y Tarifas por Sede.xlsx", sheet_name="Municipios")

print(comparativo.columns)

dtypes = {
    'Celular 2': pl.Utf8,
    # Si tienes otras columnas que causan problemas, agrégalas aquí.
    # Por ejemplo: 'Otra Columna Numerica Larga': pl.Utf8
}

enero = pd.read_excel("BD enero PROINSALUD 2025.xlsx", sheet_name="BD ENERO")
febrero = pd.read_excel("BD FEBRERO 2025.xlsx", sheet_name="Hoja1")
marzo = pd.read_excel("BD AFILIADOS MARZO 2025.xlsx", sheet_name="Hoja1")
abril = pd.read_excel("BD AFILIADOS ABRIL 2025 (1).xlsx", sheet_name="BD ABRIL")
mayo = pd.read_excel("BD AFILIADOS MAYO 2025.xlsx", sheet_name="BD")
junio = pd.read_excel("BD AFILIADOS JUNIO 2025 (1).xlsx", sheet_name="BD")
julio = pd.read_excel("BD JULIO 2025 (1).xlsx", sheet_name="Hoja1")

# enero = pl.read_excel("BD enero PROINSALUD 2025.xlsx", sheet_name="BD ENERO", schema_overrides=dtypes).lazy()
# febrero = pl.read_excel("BD FEBRERO 2025.xlsx", sheet_name="Hoja1", schema_overrides=dtypes).lazy()
# marzo = pl.read_excel("BD AFILIADOS MARZO 2025.xlsx", sheet_name="Hoja1", schema_overrides=dtypes).lazy()
# abril = pl.read_excel("BD AFILIADOS ABRIL 2025 (1).xlsx", sheet_name="Hoja1", schema_overrides=dtypes).lazy()
# mayo = pl.read_excel("BD AFILIADOS MAYO 2025.xlsx", sheet_name="BD", schema_overrides=dtypes).lazy()
# junio = pl.read_excel("BD AFILIADOS JUNIO 2025 (1).xlsx", sheet_name="BD", schema_overrides=dtypes).lazy()
# julio = pl.read_excel("BD JULIO 2025 (1).xlsx", sheet_name="Hoja1", schema_overrides=dtypes).lazy()


td_enero = enero.groupby("Mun Horus")["Primer Nombre"].count()
td_enero = pd.DataFrame(td_enero)
td_enero["Mes"] = "Enero"

td_febrero = febrero.groupby("Mun Horus")["Primer Nombre"].count()
td_febrero = pd.DataFrame(td_febrero)
td_febrero["Mes"] = "Febrero"

td_marzo = marzo.groupby("Municipio_horus")["Primer Nombre"].count()
td_marzo = pd.DataFrame(td_marzo)
td_marzo["Mes"] = "Marzo"

td_abril = abril.groupby("MUNICIPIO_HORUS")["Primer Nombre"].count()
td_abril = pd.DataFrame(td_abril)
td_abril["Mes"] = "Abril"

td_mayo = mayo.groupby("municipio_atencion_Horus")["Primer Nombre"].count()
td_mayo = pd.DataFrame(td_mayo)
td_mayo["Mes"] = "Mayo"

td_junio = junio.groupby("MUNICIPIO_ATENCIÓN_HORUS")["Primer Nombre"].count()
td_junio = pd.DataFrame(td_junio)
td_junio["Mes"] = "Junio"

td_julio = julio.groupby("MUNICIPIO_HORUS")["Primer Nombre"].count()
td_julio = pd.DataFrame(td_julio)
td_julio["Mes"] = "Julio"

total = pd.concat([td_enero, td_febrero, td_marzo, td_abril, td_mayo, td_junio, td_julio])
total = total.reset_index()
total = total.rename(columns={"index": "Municipio"})
total = total.rename(columns={"Primer Nombre": "Cantidad"})

total_combinado = pd.merge(total, municipios, on="Municipio", how="left")

# total_combinado.to_excel("total.xlsx", index=False)

comparativo_unpivote = comparativo.melt(id_vars=["DISTRIBUCION", "SERVICIO CONTRATADO"], var_name="Municipio", value_name="Tarifa")

df_final = pd.merge(comparativo_unpivote, total_combinado, left_on="Municipio", right_on="Municipio comparativo", how="left")
df_final["Total Ingreso"] = df_final["Cantidad"] * df_final["Tarifa"]
df_final["Total Ingreso"] = df_final["Total Ingreso"].round(2)

td_total = df_final.groupby("Mes")["Total Ingreso"].sum().reset_index()

td_distribucion = df_final.pivot_table(index=["Municipio_y","DISTRIBUCION"], columns="Mes", values="Total Ingreso", aggfunc="sum").reset_index()

with pd.ExcelWriter("df_final.xlsx") as writer:
    df_final.to_excel(writer, sheet_name="Base", index=False)
    td_total.to_excel(writer, sheet_name="Total", index=False)
    td_distribucion.to_excel(writer, sheet_name="Distribucion", index=False)

