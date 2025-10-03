from functools import reduce #02/10/2025
from lxml import etree
from datetime import datetime
import calendar
import string
from collections import defaultdict
import pandas as pd
import cx_Oracle
from utils.configuration import BaseConfig

def generar_conexion(ambiente):
    # FUENTE DE DATOS
    if ambiente == 1:
        bd_ip = '192.168.7.95'
        bd_sid = 'BDITSADE'
    else:
        bd_ip = '192.168.7.81'
        bd_sid = 'BDITSA'

    bd_puerto = '1521'
    bd_usuario = 'USR_ITSA_CONSULTAS'
    bd_clave = 'USR_ITSA_CONSULTAS'

    try:
        # GENERA CONEXION 
        dsn_tns = cx_Oracle.makedsn(bd_ip, bd_puerto, bd_sid)
        connection = cx_Oracle.connect(bd_usuario, bd_clave, dsn_tns)
        estado = True
    except:
        estado = False
        connection = None

    return connection, estado
 
def format_date(date):
    return date.strftime("%d/%m/%Y")
 
def get_month_range(year, month):
    last_day = calendar.monthrange(year, month)[1]
    start_date = datetime(year, month, 1)
    end_date = datetime(year, month, last_day)
    return start_date, end_date
 
def get_compras_data(conexion, year, month):
    start_date, end_date = get_month_range(year, month)
   
    query = """
    SELECT
        MOVCDOCUFISIFECH, AGECODIGO, TIOCODIGO, TIMCODIGO, MOVCNUMERO, SUSCODIGO,
        MOVCDOCUORIGCONTINTE, TIDTCODIGO, CLINUMDOCIDENTIDAD, MOVCDOCUFISINUMESERI,
        MOVCDOCUFISINUME, MOVCDOCUFISIAUTO, MOVCVALORSUBNOBIVABIEN, MOVCVALORSUBNOBIVASERV,
        MOVCVALORIVABIEN, MOVCVALORSUBSINIVABIEN, MOVCVALORSUBCONIVABIEN, MOVCVALORIVASERV,
        MOVCVALORSUBSINIVASERV, MOVCVALORSUBCONIVASERV, MOVCVALORDESCUENTO, MOVCVALORICE, LINCODIGO,
        MOVCAUTMODIFICADO, MOVcSECMODIFICADO, MOVCSERIEMODIFICADO, MOVCTIPODOCMODIFICADO
    FROM ADTRA_PROVEEDOR.PRO_MOVI_CABE
    WHERE MOVCDOCUFISIFECH BETWEEN :start_date AND :end_date
    AND MOVCESTADOREGISTRO = 'ACT'
    AND SUSCODIGO IS NOT NULL
    AND (TIMCODIGO = '01' OR TIMCODIGO = '04' OR TIMCODIGO = '05' OR TIMCODIGO = '02')
    AND NOT REGEXP_LIKE(MOVCDOCUFISINUME, '[A-Za-z]')
    AND NOT REGEXP_LIKE(MOVCDOCUFISIAUTO, '^9+$')
    """ #las ultimas 2 condiciones es porque son compras del exterior y tiene letras MOVCDOCUFISINUME y MOVCDOCUFISIAUTO no validos
 
    #result = conexion.execute(query, {"start_date": start_date, "end_date": end_date})
    #return result.fetchall()
    df = pd.read_sql(query, conexion, params={"start_date": start_date, "end_date": end_date})
    return df.to_dict('records') if not df.empty else []
 
def get_ventas_data(conexion, year, month):
    start_date, end_date = get_month_range(year, month)
 
    #FAVCFACTFISISERI  
    query = """
        SELECT LPH.AGECODIGO, LPH.FAVCNUMERO, LPH.FAVCLETRA, LPH.FAVCFACTFISISERI,
               LPH.FAVCFACTFISINUME, LPH.FAVCFECHA, LPH.CLINUMDOCIDENTIDAD,
               LPH.FAVCVALORIVA, LPH.FAVCSUBTOTAL, LPH.FAVCVALORDESCESPE, LPH.FAVCLETRAREFERENCIA,
               CLT.CLITIPODOCUMENTO, LPH.FAVCESTADO, LPH.FAVCSUBTOTALCONIVA, LPH.FAVCVALORDESCLINEACONIVA,
               LPH.FAVCVALORDESXPAGOCONIVA, LPH.FAVCVALORDESCCONTCONIVA, LPH.FAVCVALORDESCESPECONIVA
        FROM ADTRA_LPH.LPH_FACTURA_CABE LPH
        JOIN NUCLEO.ADM_CLIENTE CLT ON LPH.CLINUMDOCIDENTIDAD = CLT.CLINUMDOCIDENTIDAD
        WHERE LPH.FAVCFECHA BETWEEN :start_date AND :end_date
              AND LPH.FAVCESTADO <> 'A'
    """

    df = pd.read_sql(query, conexion, params={"start_date": start_date, "end_date": end_date})
    return df.to_dict("records") if not df.empty else []
 
def get_retenciones(conexion, agecodigo, tiocodigo, timcodigo, movcnumero,year):
    try:
        query = f"""
        SELECT
            D.RETCODIGO, D.MBDBASERETENCION, D.MBDVALORN, R.RETPORCENTAJE, D.MBCNUMERO, R.RETTIPORETENCION
        FROM
            ADTRA_BANCAJCAR.BAN_MOVIM_BANCO_FACTURAS F
            JOIN ADTRA_BANCAJCAR.BAN_MOVIM_BANCO_DETALLE D ON F.MBCFAGECODPRV = D.AGECODIGO
                AND F.MBCTIPOMOVIMIENTO = D.MBCTIPOMOVIMIENTO
                AND F.MBCNUMERO = D.MBCNUMERO
            JOIN ADTRA_BANCAJCAR.BAN_RETENCION R ON D.RETCODIGO = R.RETCODIGO AND D.RETANIO = R.RETANIO
        WHERE
            F.MBCFAGECODPRV = :agecodigo
            AND F.MBCFTIPORIPRV = :tiocodigo
            AND F.MBCFTIPMOVPRV = :timcodigo
            AND F.MBCFNUMINTPRV = :movcnumero
            AND R.RETANIO = '{year}'
        """
    
        df = pd.read_sql(query, conexion, params={
            "agecodigo": agecodigo,
            "tiocodigo": tiocodigo,
            "timcodigo": timcodigo,
            "movcnumero": movcnumero
        })

        if df.empty:
            return []

        mbcnumero_ref = df.loc[0, 'MBCNUMERO']
        df_filtered = df[df['MBCNUMERO'] == mbcnumero_ref]

        return df_filtered.to_dict('records')
   
    except Exception as e:
        print(f"Error al obtener retenciones: {e}")
        return []

def get_valores_faltantes(conexion, mbcnumero, year):
    try:
        query1 = """
        SELECT
            DR.MBCRPORCENTAJE, DR.MBCRVALORRETENIDO
        FROM
            ADTRA_BANCAJCAR.BAN_MOVIM_BANCO_DET_RETEN DR
        WHERE
            DR.MBCNUMERO = :mbcnumero
            AND DR.MBCRTIPORETENCION NOT IN ('BI', 'SE')
        """

        df = pd.read_sql(query1, conexion, params={"mbcnumero": mbcnumero})

        if df.empty:
            query2 = f"""
                SELECT
                    R.RETPORCENTAJE, D.MBDBASERETENCION
                FROM
                    ADTRA_BANCAJCAR.BAN_MOVIM_BANCO_DET_RETEN DR
                    JOIN ADTRA_BANCAJCAR.BAN_MOVIM_BANCO_DETALLE D ON D.MBCNUMERO = DR.MBCNUMERO
                    JOIN ADTRA_BANCAJCAR.BAN_RETENCION R ON D.RETCODIGO = R.RETCODIGO AND D.RETANIO = R.RETANIO
                WHERE
                    DR.MBCNUMERO = :mbcnumero
                    AND R.RETTIPORETENCION NOT IN ('BI', 'SE')
                    AND R.RETANIO = :year
            """

            df = pd.read_sql(query2, conexion, params={"mbcnumero": mbcnumero, "year": str(year)})

        return df.iloc[0].tolist() if not df.empty else []

    except Exception as e:
        print(f"Error al obtener los valores faltantes: {e}")
        return []

def ObtenerDocumentoAfectado(conexion, agecodigo, tiocodigo, timcodigo, movcnumero):
    try:
        query = """
        SELECT
            PA.MOVATIMCODIGO, PC.MOVCDOCUFISINUMESERI, PC.MOVCDOCUFISINUME, PC.MOVCDOCUFISIAUTO
        FROM
            ADTRA_PROVEEDOR.PRO_MOVI_AFEC PA
            JOIN ADTRA_PROVEEDOR.PRO_MOVI_CABE PC ON PA.MOVAAGECODIGO = PC.AGECODIGO
                AND PA.MOVATIOCODIGO = PC.TIOCODIGO
                AND PA.MOVATIMCODIGO = PC.TIMCODIGO
                AND PA.MOVAMOVCNUMERO = PC.MOVCNUMERO
        WHERE
            PA.AGECODIGO = :agecodigo
            AND PA.TIOCODIGO = :tiocodigo
            AND PA.TIMCODIGO = :timcodigo
            AND PA.MOVCNUMERO = :movcnumero
        """

        df = pd.read_sql(query, conexion, params={
            "agecodigo": agecodigo,
            "tiocodigo": tiocodigo,
            "timcodigo": timcodigo,
            "movcnumero": movcnumero,
        })

        return df.iloc[0].tolist() if not df.empty else []

    except Exception as e:
        print(f"Error al obtener los valores faltantes: {e}")
        return []

def obtenerNumeroFacturas(conexion, year, month):
    start_date, end_date = get_month_range(year, month)
    query = """
        SELECT LPH.FAVCNUMERO
        FROM ADTRA_LPH.LPH_FACTURA_CABE LPH
        WHERE LPH.FAVCFECHA BETWEEN :start_date AND :end_date
          AND LPH.FAVCLETRAREFERENCIA <> 'E'
    """

    df = pd.read_sql(query, conexion, params={
        "start_date": start_date,
        "end_date": end_date
    })

    resultados_lph = df['FAVCNUMERO'].str.replace(" ", "").tolist()
    return resultados_lph


def obtenerLista(conexion, numclientId, year, month, resultados_lph):
    start_date, end_date = get_month_range(year, month)
    resultados_nce = []  # nota credito electronica
    resultados_ncf = []  # nota credito fisica
    try:
        query = """
            SELECT CCC.NCRSUBTOTAL, CCC.NCRIVA, CCC.NCRNUMEROCREDITO
            FROM ADTRA_BANCAJCAR.CAR_NOTA_CREDITO_CABECERA CCC
            WHERE CCC.NCRFECHACREA BETWEEN :start_date AND :end_date
              AND CCC.CLINUMDOCIDENTIDAD = :numclientId
              AND CCC.NCRIVA <> 0
        """

        df = pd.read_sql(query, conexion, params={
            "start_date": start_date,
            "end_date": end_date,
            "numclientId": numclientId
        })

        for _, row in df.iterrows():
            nro_credito = row['NCRNUMEROCREDITO']
            if "D" in nro_credito and nro_credito.split("D")[1].replace(" ", "") in resultados_lph:
                resultados_nce.append((row['NCRSUBTOTAL'], row['NCRIVA'], nro_credito))
            else:
                resultados_ncf.append((row['NCRSUBTOTAL'], row['NCRIVA'], nro_credito))

        return [obtenerSubIvaNum(resultados_nce), obtenerSubIvaNum(resultados_ncf)]

    except Exception as e:
        print(f"Error al obtener los valores: {e} Cliente: {numclientId}")
        return []

def obtenerSubIvaNum(resultados):
    if len(resultados) != 0:
        subTotal = float(reduce(lambda x,y: x+y, [float(resultado[0]) for resultado in resultados]))
        iva = float(reduce(lambda x,y: x+y, [float(resultado[1]) for resultado in resultados]))
        return [subTotal, iva, len(resultados)]
    return []


def retornarRet(conexion, cliente, year, month):
    start_date, end_date = get_month_range(year, month)
    query = """
        SELECT CCC.NCRNUMEROCREDITO, CCD.NCRRRET1, CCD.NCRRRET2, CCD.NCRRRET10, CCD.NCRRRET20, CCD.NCRRRET30, CCD.NCRRRET70, CCD.NCRRRET100, LPH.FAVCNUMERO
        FROM ADTRA_BANCAJCAR.CAR_NOTA_CREDITO_CABECERA CCC
        JOIN ADTRA_BANCAJCAR.CAR_NOTA_CREDITO_DETRET CCD ON CCC.NCRNUMEROCREDITO = CCD.NCRNUMEROCREDITO
        JOIN ADTRA_LPH.LPH_FACTURA_CABE LPH ON CCD.NCRRBASERET10 = LPH.FAVCVALORIVA AND CCC.CLINUMDOCIDENTIDAD = LPH.CLINUMDOCIDENTIDAD
        WHERE LPH.FAVCFECHA BETWEEN :start_date AND :end_date
          AND CCC.CLINUMDOCIDENTIDAD = :cliente
          AND CCC.NCRFECHA >= LPH.FAVCFECHA
          AND LPH.FAVCLETRAREFERENCIA <> 'I'
          AND CCC.NCRESTADO <> 'A'
          AND LPH.FAVCESTADO NOT IN ('A', 'X')
    """

    df = pd.read_sql(query, conexion, params={
        "start_date": start_date,
        "end_date": end_date,
        "cliente": cliente
    })

    renta = 0.0
    iva = 0.0
    numerosFavc = set()

    for _, row in df.iterrows():
        if row['FAVCNUMERO'] not in numerosFavc:
            renta += float(row['NCRRRET1'] + row['NCRRRET2'])
            iva += float(row['NCRRRET10'] + row['NCRRRET20'] + row['NCRRRET30'] + row['NCRRRET70'] + row['NCRRRET100'])
            numerosFavc.add(row['FAVCNUMERO'])

    return [iva, renta]

def generar_ventas_establecimiento(root, diccionario_ventas_establecimiento):
    ventas_establecimiento = etree.SubElement(root, 'ventasEstablecimiento')
    total_ventas = 0.0
    for elemento in sorted(diccionario_ventas_establecimiento.keys()):
        ventEst = etree.SubElement(ventas_establecimiento, "ventaEst")
        #etree.SubElement(ventEst, "codEstab").text = elemento
        etree.SubElement(ventEst, "codEstab").text = elemento[:3]
        etree.SubElement(ventEst, "ventasEstab").text = f"{diccionario_ventas_establecimiento[elemento]:.2f}"
        total_ventas += diccionario_ventas_establecimiento[elemento]
    return total_ventas

def generar_compras(conexion, root, year, compras_data):
    # Compras
    compras_element = etree.SubElement(root, 'compras')
    for compra in compras_data:
        if compra['TIDTCODIGO'] == '03': #Si el proveedor tiene pasaporte no incluir
            continue
        detalle_compras = etree.SubElement(compras_element, 'detalleCompras')
       
        # Mapeo de datos
        etree.SubElement(detalle_compras, 'codSustento').text = compra['SUSCODIGO']  # SUSCODIGO
        etree.SubElement(detalle_compras, 'tpIdProv').text = compra['TIDTCODIGO']  # TIDTCODIGO
        etree.SubElement(detalle_compras, 'idProv').text = compra['CLINUMDOCIDENTIDAD']  # CLINUMDOCIDENTIDAD
        etree.SubElement(detalle_compras, 'tipoComprobante').text = compra['TIMCODIGO']  # TIMCODIGO
        etree.SubElement(detalle_compras, 'parteRel').text = 'SI'
       
        # Fecha registro
        fecha_registro = format_date(compra['MOVCDOCUFISIFECH'])  # MOVCDOCUFISIFECH
        etree.SubElement(detalle_compras, 'fechaRegistro').text = fecha_registro

        # Establecimiento, punto de emisión y secuencial
        numeseri = str(compra['MOVCDOCUFISINUMESERI'])
        etree.SubElement(detalle_compras, 'establecimiento').text = numeseri[:3]
        etree.SubElement(detalle_compras, 'puntoEmision').text = numeseri[3:6]
        etree.SubElement(detalle_compras, 'secuencial').text = str(compra['MOVCDOCUFISINUME']).zfill(9)
        
        #Fecha emision
        etree.SubElement(detalle_compras, 'fechaEmision').text = fecha_registro
       
        # Autorización
        etree.SubElement(detalle_compras, 'autorizacion').text = str(compra['MOVCDOCUFISIAUTO'].strip())
       
        # Valores
        base_no_gra_iva = float(compra['MOVCVALORSUBNOBIVABIEN']) + float(compra['MOVCVALORSUBNOBIVASERV']) 
        etree.SubElement(detalle_compras, 'baseNoGraIva').text = f"{base_no_gra_iva:.2f}"
       
        #Base imponible
        ValorbaseImponible = 0
        ValorbaseImpGrav = 0

        if float(compra['MOVCVALORIVABIEN']) == 0:
            ValorbaseImponible += float(compra['MOVCVALORSUBSINIVABIEN'])
        else:
            ValorbaseImpGrav += float(compra['MOVCVALORSUBCONIVABIEN'])
       
        if float(compra['MOVCVALORIVASERV']) == 0:
            ValorbaseImponible += float(compra['MOVCVALORSUBSINIVASERV'])
        else:
            ValorbaseImpGrav += float(compra['MOVCVALORSUBCONIVASERV'])
       
        Valor = float(compra['MOVCVALORSUBNOBIVABIEN']) + float(compra['MOVCVALORSUBNOBIVASERV'])
       
        MOVCValorDescuento = float(compra['MOVCVALORDESCUENTO']) if pd.notna(compra['MOVCVALORDESCUENTO']) else 0.0
    
        if MOVCValorDescuento is not None and MOVCValorDescuento != 0:
            if ValorbaseImponible >= MOVCValorDescuento:
                ValorbaseImponible -= MOVCValorDescuento
            else:
                MOVCValorDescuento -= ValorbaseImponible
                ValorbaseImponible = 0
       
            if ValorbaseImpGrav >= MOVCValorDescuento:
                ValorbaseImpGrav -= MOVCValorDescuento
            else:
                MOVCValorDescuento -= ValorbaseImpGrav
                ValorbaseImpGrav = 0

            if Valor >= MOVCValorDescuento:
                Valor -= MOVCValorDescuento
            else:
                MOVCValorDescuento -= Valor
                Valor = 0
       
        etree.SubElement(detalle_compras, 'baseImponible').text = f"{ValorbaseImponible:.2f}"
        etree.SubElement(detalle_compras, 'baseImpGrav').text = f"{ValorbaseImpGrav:.2f}"
        etree.SubElement(detalle_compras, 'baseImpExe').text = "0.00"  # Valor fijo ?
        #montoIce = 0.0 if compra['MOVCVALORICE'] is None else float(compra['MOVCVALORICE'])
        montoIce = 0.0 if pd.isna(compra['MOVCVALORICE']) else float(compra['MOVCVALORICE'])
        etree.SubElement(detalle_compras, 'montoIce').text = f"{montoIce:.2f}"  # MOVCVALORICE
        etree.SubElement(detalle_compras, 'montoIva').text = f"{float(compra['MOVCVALORIVABIEN']) + float(compra['MOVCVALORIVASERV']):.2f}"  # MOVCVALORIVABIEN + MOVCVALORIVASERV
       
        # Sección de retenciones (AIR)
        retenciones = get_retenciones(conexion, compra['AGECODIGO'], compra['TIOCODIGO'], compra['TIMCODIGO'], compra['MOVCNUMERO'], year)

        if retenciones:
            air_element = None
            valorRetBien10 = 0.0
            valRetServ20 = 0.0
            valorRetBienes = 0.0
            valRetServ50 = 0.0
            valorRetServicios = 0.0
            valRetServ100 = 0.0  
            
            for ret in retenciones: 
                valores_faltantes = get_valores_faltantes(conexion, ret['MBCNUMERO'], year)
                if len(valores_faltantes) != 0:
                    tipo = valores_faltantes[0]
                    valor = valores_faltantes[1]
                    if ret['RETTIPORETENCION'] not in ('BI','SE'):
                        if tipo == 10:
                            valorRetBien10 += float(valor)
                        elif tipo == 20:
                            valRetServ20 += float(valor)
                        elif tipo == 30:
                            valorRetBienes += float(valor)
                        elif tipo == 50:
                            valRetServ50 += float(valor)
                        elif tipo == 70:
                            valorRetServicios += float(valor)
                        elif tipo == 100:
                            valRetServ100 += float(valor)
    
            etree.SubElement(detalle_compras, 'valRetBien10').text = f"{valorRetBien10:.2f}"
            etree.SubElement(detalle_compras, 'valRetServ20').text = f"{valRetServ20:.2f}"
            etree.SubElement(detalle_compras, 'valorRetBienes').text = f"{valorRetBienes:.2f}"
            etree.SubElement(detalle_compras, 'valRetServ50').text = f"{valRetServ50:.2f}"
            etree.SubElement(detalle_compras, 'valorRetServicios').text = f"{valorRetServicios:.2f}"
            etree.SubElement(detalle_compras, 'valRetServ100').text = f"{valRetServ100:.2f}"

            etree.SubElement(detalle_compras, 'totbasesImpReemb').text = "0.00"  # Valor fijo ?
            
            pago_exterior(detalle_compras) #seccion pagoExterior
            
            # Sección Formas Pago
            Valor =  compra['MOVCVALORSUBSINIVABIEN'] + compra['MOVCVALORSUBSINIVASERV'] + compra['MOVCVALORSUBNOBIVABIEN'] + compra['MOVCVALORSUBNOBIVASERV'] + float(ValorbaseImponible) + float(ValorbaseImpGrav) + compra['MOVCVALORIVABIEN'] + compra['MOVCVALORIVASERV'] + float(montoIce)

            if Valor > 500: #antes de 20 de diciembre de 2023  era > 1000
                formas_pago = etree.SubElement(detalle_compras, 'formasDePago')
                etree.SubElement(formas_pago, 'formaPago').text = '20'

            # Agrupar las retenciones por codRetAir 
            retenciones_agrupadas = defaultdict(lambda: {"base": 0.0, "valor": 0.0, "porcentaje": None, "tipo": ''})
            for ret in retenciones:
                cod_ret = ret['RETCODIGO']
                base = float(ret['MBDBASERETENCION'])
                valor = float(ret['MBDVALORN'])
                porcentaje = ret['RETPORCENTAJE']

                retenciones_agrupadas[cod_ret]["base"] += base
                retenciones_agrupadas[cod_ret]["valor"] += valor
                if retenciones_agrupadas[cod_ret]["porcentaje"] is None:
                    retenciones_agrupadas[cod_ret]["porcentaje"] = porcentaje
                retenciones_agrupadas[cod_ret]["tipo"] = ret['RETTIPORETENCION']

            # Generar los elementos XML una sola vez por código
            if retenciones and air_element is None:
                air_element = etree.SubElement(detalle_compras, 'air')

            for cod_ret, datos in retenciones_agrupadas.items():
                if (datos['tipo'] in ('BI', 'SE')):
                    detalle_air = etree.SubElement(air_element, 'detalleAir')
                    etree.SubElement(detalle_air, 'codRetAir').text = cod_ret
                    etree.SubElement(detalle_air, 'baseImpAir').text = f"{datos['base']:.2f}"
                    etree.SubElement(detalle_air, 'porcentajeAir').text = f"{datos['porcentaje']:.2f}"
                    etree.SubElement(detalle_air, 'valRetAir').text = f"{datos['valor']:.2f}"
 
        else:
            etree.SubElement(detalle_compras, 'valRetBien10').text = "0.00"
            etree.SubElement(detalle_compras, 'valRetServ20').text = "0.00"
            etree.SubElement(detalle_compras, 'valorRetBienes').text = "0.00"
            etree.SubElement(detalle_compras, 'valRetServ50').text = "0.00"
            etree.SubElement(detalle_compras, 'valorRetServicios').text = "0.00"
            etree.SubElement(detalle_compras, 'valRetServ100').text = "0.00"

            etree.SubElement(detalle_compras, 'totbasesImpReemb').text = "0.00"  # Valor fijo ?
            
            pago_exterior(detalle_compras) #seccion pagoExterior
               
            # Sección Formas Pago
            Valor =  compra['MOVCVALORSUBSINIVABIEN'] + compra['MOVCVALORSUBSINIVASERV'] + compra['MOVCVALORSUBNOBIVABIEN'] + compra['MOVCVALORSUBNOBIVASERV'] + float(ValorbaseImponible) + float(ValorbaseImpGrav) + compra['MOVCVALORIVABIEN'] + compra['MOVCVALORIVASERV'] + float(montoIce)
 
            if Valor > 500:
                formas_pago = etree.SubElement(detalle_compras, 'formasDePago')
                etree.SubElement(formas_pago, 'formaPago').text = '20'
           
            etree.SubElement(detalle_compras, 'air').text = ""

            if compra['TIMCODIGO'] == '04':
                if (str(compra['MOVCAUTMODIFICADO']).strip() != '' and 
                    str(compra['MOVCSECMODIFICADO']).strip() != '' and 
                    str(compra['MOVCSERIEMODIFICADO']).strip() != '' and 
                    str(compra['MOVCTIPODOCMODIFICADO']).strip() != ''):
                    
                    etree.SubElement(detalle_compras, 'docModificado').text = str(compra['MOVCTIPODOCMODIFICADO'])
                    etree.SubElement(detalle_compras, 'estabModificado').text = str(compra['MOVCSERIEMODIFICADO'])[:3]
                    etree.SubElement(detalle_compras, 'ptoEmiModificado').text = str(compra['MOVCSERIEMODIFICADO'])[3:6]
                    etree.SubElement(detalle_compras, 'secModificado').text = str(compra['MOVCSECMODIFICADO'])
                    etree.SubElement(detalle_compras, 'autModificado').text = str(compra['MOVCAUTMODIFICADO']).zfill(9)
                else:
                    valoresCompletados = ObtenerDocumentoAfectado(
                        conexion, 
                        compra['AGECODIGO'], 
                        compra['TIOCODIGO'], 
                        compra['TIMCODIGO'], 
                        compra['MOVCNUMERO']
                    )
                    if valoresCompletados:
                        etree.SubElement(detalle_compras, 'docModificado').text = str(valoresCompletados[0])
                        etree.SubElement(detalle_compras, 'estabModificado').text = str(valoresCompletados[1])[:3]
                        etree.SubElement(detalle_compras, 'ptoEmiModificado').text = str(valoresCompletados[1])[3:6]
                        etree.SubElement(detalle_compras, 'secModificado').text = str(valoresCompletados[2])
                        etree.SubElement(detalle_compras, 'autModificado').text = str(valoresCompletados[3]).zfill(9)

def generar_ventas(conexion, ventas_data, root, year, month):
    # Ventas
    ventas_element = etree.SubElement(root, 'ventas')
    clientes_procesados = {}
    clientes_nota_credito_fisica = {}
    clientes_nota_credito_electrica = {}
    diccionario_ventas_establecimiento = {}

    resultados_lph = obtenerNumeroFacturas(conexion, year, month)

    for row in ventas_data:
        id_cliente = row['CLINUMDOCIDENTIDAD']
        if row['CLITIPODOCUMENTO'] == "C":
            tp_id_cliente = "05"
        elif row['CLITIPODOCUMENTO'] == "R":
            tp_id_cliente = "04"
        elif row['CLITIPODOCUMENTO'] == "P":
            tp_id_cliente = "06"

        base_imp_grav = float(row['FAVCSUBTOTALCONIVA']) - float(row['FAVCVALORDESCLINEACONIVA']) - float(row['FAVCVALORDESXPAGOCONIVA']) - float(row['FAVCVALORDESCCONTCONIVA']) - float(row['FAVCVALORDESCESPECONIVA'])
        monto_iva = row['FAVCVALORIVA']  # FAVCVALORIVA
        tipo_emision = row['FAVCLETRAREFERENCIA']  # FAVCLETRAREFERENCIA
        estado = row['FAVCESTADO']  # FAVCESTADO
        #codEstablecimiento = row[3][:3]
        codEstablecimiento = row['FAVCFACTFISISERI']
        valoresRet = retornarRet(conexion, id_cliente, year, month)
       
        if codEstablecimiento not in diccionario_ventas_establecimiento.keys():
            diccionario_ventas_establecimiento[codEstablecimiento] = 0.0
 
        listas = obtenerLista(conexion, id_cliente, year, month, resultados_lph)
        listaNCE = listas[0]  #lista de notas de credito electronicas
        listaNCF = listas[1]  #lista de notas de credito fisicas
 
        if len(listaNCF) > 0:
            if id_cliente not in clientes_nota_credito_fisica:
                clientes_nota_credito_fisica[id_cliente] = {
                    "tpIdCliente": tp_id_cliente,
                    "idCliente": id_cliente,
                    "numeroComprobantes": 0,
                    "baseImpGrav": 0.0,
                    "montoIva": 0.0,
                    "tipoEmision": 'F',
                }
                clientes_nota_credito_fisica[id_cliente]["numeroComprobantes"] =  listaNCF[2]
                clientes_nota_credito_fisica[id_cliente]["baseImpGrav"] = listaNCF[0]
                clientes_nota_credito_fisica[id_cliente]["montoIva"] = listaNCF[1]
 
                diccionario_ventas_establecimiento[codEstablecimiento] -= listaNCF[0]
 
        if len(listaNCE) > 0:
            if id_cliente not in clientes_nota_credito_electrica:
                clientes_nota_credito_electrica[id_cliente] = {
                    "tpIdCliente": tp_id_cliente,
                    "idCliente": id_cliente,
                    "numeroComprobantes": 0,
                    "baseImpGrav": 0.0,
                    "montoIva": 0.0,
                    "tipoEmision": 'E',
                }
                clientes_nota_credito_electrica[id_cliente]["numeroComprobantes"] =  listaNCE[2]
                clientes_nota_credito_electrica[id_cliente]["baseImpGrav"] = listaNCE[0]
                clientes_nota_credito_electrica[id_cliente]["montoIva"] = listaNCE[1]

        if id_cliente not in clientes_procesados and tipo_emision != 'I':
            clientes_procesados[id_cliente] = {
                    "tpIdCliente": tp_id_cliente,
                    "idCliente": id_cliente,
                    "numeroComprobantes": 0,
                    "baseImpGrav": 0.0,
                    "montoIva": 0.0,
                    "tipoEmision": "E",
                    "retIva": valoresRet[0],
                    "retRent": valoresRet[1]
            }
        if id_cliente in clientes_procesados and row['FAVCLETRAREFERENCIA'] != 'I': #row[10] es FAVCLETRAREFERENCIA
            clientes_procesados[id_cliente]["numeroComprobantes"] += 1
            clientes_procesados[id_cliente]["baseImpGrav"] += float(base_imp_grav)
            clientes_procesados[id_cliente]["montoIva"] += float(monto_iva)

    # Generar XML para ventas normales
    for cliente, valores in clientes_procesados.items():
        agregar_detalle_venta(ventas_element, cliente, valores, "18") 

    # Generar XML para ventas anuladas
    for cliente, valores in clientes_nota_credito_electrica.items():
        agregar_detalle_venta(ventas_element, cliente, valores, "04") 

    for cliente, valores in clientes_nota_credito_fisica.items():
        agregar_detalle_venta(ventas_element, cliente, valores, "04") 
    
    return diccionario_ventas_establecimiento

def agregar_detalle_venta(xml_padre, cliente, valores_cliente, tipo_comprobante): #tipo comprobante: "04" -> nota de credito, "18" -> venta normal
    detalle_ventas = etree.SubElement(xml_padre, "detalleVentas")
    etree.SubElement(detalle_ventas, "tpIdCliente").text = valores_cliente["tpIdCliente"]
    etree.SubElement(detalle_ventas, "idCliente").text = cliente
    etree.SubElement(detalle_ventas, "parteRelVtas").text = "NO"
    etree.SubElement(detalle_ventas, "tipoComprobante").text = tipo_comprobante
    etree.SubElement(detalle_ventas, "tipoEmision").text = valores_cliente["tipoEmision"]
    etree.SubElement(detalle_ventas, "numeroComprobantes").text = str(valores_cliente["numeroComprobantes"])
    etree.SubElement(detalle_ventas, "baseNoGraIva").text = "0.00"
    etree.SubElement(detalle_ventas, "baseImponible").text = "0.00"
    etree.SubElement(detalle_ventas, "baseImpGrav").text = f"{valores_cliente['baseImpGrav']:.2f}"
    etree.SubElement(detalle_ventas, "montoIva").text = f"{valores_cliente['montoIva']:.2f}"
    etree.SubElement(detalle_ventas, "montoIce").text = "0.00"
    etree.SubElement(detalle_ventas, "valorRetIva").text = f"{valores_cliente.get('retIva', 0.00):.2f}"
    etree.SubElement(detalle_ventas, "valorRetRenta").text = f"{valores_cliente.get('retRent', 0.00):.2f}"

    if tipo_comprobante != '04':
        formas_pago = etree.SubElement(detalle_ventas, 'formasDePago')
        if valores_cliente["baseImpGrav"] > 1000:
            etree.SubElement(formas_pago, 'formaPago').text = '20'
        else:
            etree.SubElement(formas_pago, 'formaPago').text = '01'

def pago_exterior(xml_padre):
    pago_exterior = etree.SubElement(xml_padre, 'pagoExterior')
    etree.SubElement(pago_exterior, 'pagoLocExt').text = '01'
    etree.SubElement(pago_exterior, 'paisEfecPago').text = 'NA'
    etree.SubElement(pago_exterior, 'aplicConvDobTrib').text = 'NA'
    etree.SubElement(pago_exterior, 'pagExtSujRetNorLeg').text = 'NA'

def get_empresa_data (conexion, empresa):
    query = """
        SELECT
            CLIAPELLIDO, CLINOMBRE, CLITIPODOCUMENTO
        FROM
            NUCLEO.ADM_CLIENTE
        WHERE
            CLINUMDOCIDENTIDAD = :empresa
        """
    df = pd.read_sql(query, conexion, params={"empresa":empresa})
    return df.to_dict('records') if not df.empty else []

def create_xml(empresa, ambiente, month, year):
    conexion, estado = generar_conexion(ambiente)
    if not estado:
        print("No se pudo conectar a la base de datos")
        return
    empresa_data = get_empresa_data(conexion, empresa)[0]
    root = etree.Element('iva')
    # Datos generales
    etree.SubElement(root, 'TipoIDInformante').text = empresa_data["CLITIPODOCUMENTO"]
    etree.SubElement(root, 'IdInformante').text = empresa
    nombre_limpio = ''.join(c for c in empresa_data['CLINOMBRE'] if c not in string.punctuation)
    etree.SubElement(root, 'razonSocial').text = f"{empresa_data['CLIAPELLIDO']} {nombre_limpio}"
    etree.SubElement(root, 'Anio').text = str(year)  
    etree.SubElement(root, 'Mes').text = f"{month:02d}"
    #etree.SubElement(root, 'numEstabRuc').text = config.get("ATS_VENTURE", "NUM_ESTABLECIMIENTO_RUC")
    etree.SubElement(root, 'numEstabRuc').text = BaseConfig.NUM_ESTABLECIMIENTO_RUC_VENTURE
    totalVentas = etree.SubElement(root, 'totalVentas')
    etree.SubElement(root, 'codigoOperativo').text = 'IVA'
   
    # Compras
    compras_data = get_compras_data(conexion, year, month)

    generar_compras(conexion, root, year, compras_data)
 
    # Ventas
    ventas_data = get_ventas_data(conexion, year, month)
    diccionario_ventas_establecimiento = generar_ventas(conexion, ventas_data, root, year, month)
   
    # Ventas Establecimiento
    total_ventas = generar_ventas_establecimiento(root, diccionario_ventas_establecimiento)
    
    totalVentas.text = f"{total_ventas:.2f}"

    conexion.close()

    return root

if __name__=='__main__':
    print('HOLA')
    empresa = '0190485048001'
    ambiente = 2
    year = 2025
    month = 7
    output_file = f'ATS_{year}_{month}.xml'
    
    root = create_xml(empresa,ambiente,month,year)
    tree = etree.ElementTree(root)
    tree.write(output_file, pretty_print=True, xml_declaration=True, encoding='UTF-8')
    