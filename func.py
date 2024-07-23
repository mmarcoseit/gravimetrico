#
# oci-objectstorage-get-object version 1.0.
#
# Copyright (c) 2020 Oracle, Inc.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
#

import io
#
# oci-objectstorage-get-object version 1.0.
#
# Copyright (c) 2020 Oracle, Inc.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
#

import io
import os
import json
import sys
import csv
from fdk import response
from datetime import datetime

import oci.object_storage


BUCKET_NAME = "bucket-gravimetrico"
OBJECT_ULT_EJECUCION= "ultEjecucion.json"
OBJECT_GRAV = "pesados.csv"
OBJECT_ERP = "erp.json"
OBJECT_RESULTADO = "resultado.json"


def handler(ctx, data: io.BytesIO=None):
    try:
        resp = mergeData()
    except Exception as e:
        updateUltEjecucion("ERROR", "Ocurrio un error en la function: " + str(e))
        raise Exception(str(e))
    return response.Response(
        ctx,
        response_data = json.dumps(resp),
        headers={"Content-Type": "application/json"}
    )

def getObject(bucketName, objectName, contentType = None):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    try:
        if(contentType):
            object = client.get_object(namespace, bucketName, objectName, http_response_content_type= contentType)
        else:
            object = client.get_object(namespace, bucketName, objectName)
        if object.status == 200:
            response = object.data
        else:
            raise Exception("Failed: The object " + objectName + " could not be retrieved. Status != 200")
    except Exception as e:
        raise Exception("GET_OBJECT_ERROR - " + str(e))
    return response


def readGravimetricoData(bucketName, objectName, dateFrom, dateTo):
    print("Running readGravimetricoData function")
    try:
        content = getObject(bucketName, objectName)
        dateFromObject = datetime.strptime(dateFrom, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
        dateToObject = datetime.strptime(dateTo, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
        lines = content.text.split("\r\n")
        consumos = {}
        for line in lines:
            if(line != ''):
                [date, item, intake] = line.split(",")
                dateGrav = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
                if(dateGrav > dateFromObject and dateGrav <= dateToObject):
                    if(item in consumos):
                        consumos[item] += float(intake)
                    else:
                        consumos[item] = float(intake)
    except Exception as e:
        raise Exception("READ_GRAVIMETRICO_DATA_ERROR - " + str(e))
    return consumos

def mergeData():
    try:
        ultEjecucion = getObject(BUCKET_NAME, OBJECT_ULT_EJECUCION,"application/json").json()
        consumosGrav = readGravimetricoData(BUCKET_NAME, OBJECT_GRAV, ultEjecucion["dateFrom"], ultEjecucion["dateTo"])
        erpDatos = getObject(BUCKET_NAME, OBJECT_ERP, "application/json").json()
        if(erpDatos is None):
            updateUltEjecucion("ERROR", "No hay datos del ERP")
            raise Exception("No hay datos del Erp")
        else:
            consumosErp = erpDatos["consumos"]
        if(consumosGrav == {}):
            updateUltEjecucion("ERROR", "No hay datos del Gravimetrico")
            raise Exception("No hay datos del Gravimetrico")
        jsonSalida = {}
        listaResultado = []
        itemsWithWo = []
        badConfigItems = []
        print("Iterating erp consumption")
        for consumo in consumosErp:
            resultado = {}
            if(str(consumo["itemNumber"]) in consumosGrav.keys()):
                itemsWithWo.append(str(consumo["itemNumber"]))
                if(consumo["uomCode"] == 'KG'):
                    resultado["itemNumber"] = consumo["itemNumber"]
                    resultado["consumoProporcional"] = ((consumosGrav[str(consumo["itemNumber"])] * consumo["pctIncidencia"]) / 100)
                    resultado["workOrderNumber"] = consumo["workOrderNumber"]
                    resultado["workOrderId"] = consumo["workOrderId"]
                    resultado["supplySubinventory"] = consumo["supplySubinventory"]
                    resultado["supplyLocatorId"] = consumo["supplyLocatorId"]
                    resultado["inventoryItemId"] = consumo["inventoryItemId"]
                    resultado["gravimetrico"] = consumosGrav[str(consumo["itemNumber"])]
                    resultado["porcentajeIncidencia"] = consumo["pctIncidencia"]
                    resultado["uomCode"] = consumo["uomCode"]
                    resultado["operationSeqNumber"] = consumo["operationSeqNumber"]
                    resultado["organizationCode"] = consumo["organizationCode"]
                    resultado["supplyLocator"] = consumo["supplyLocator"]
                    listaResultado.append(resultado)
                else:
                    item = {}
                    item["itemNumber"] = consumo["itemNumber"]
                    item["uomCode"] = consumo["uomCode"]
                    item["gravimetrico"] = consumosGrav[str(consumo["itemNumber"])]
                    if(item not in badConfigItems):
                        badConfigItems.append(item)
                    
        itemsWithoutWo = []
        for itemGrav in consumosGrav.keys():
            if str(itemGrav) not in itemsWithWo:
                item = {}
                item["itemNumber"] = itemGrav
                item["gravimetrico"] = consumosGrav[str(itemGrav)]
                itemsWithoutWo.append(item)
        jsonSalida["consumos"] = listaResultado
        jsonSalida["itemsSinWorkOrder"] = itemsWithoutWo
        jsonSalida["itemsMalConfiguradoros"] = badConfigItems
        response = writeObject(BUCKET_NAME, OBJECT_RESULTADO, jsonSalida)
        updateUltEjecucion("PENDING_TRANSACTIONS", "")
    except Exception as e:
        raise Exception("MERGE_DATA_ERROR - "  + str(e))
    return response

def updateUltEjecucion(status, errorMessage):
    try:
        ultEjecucion = getObject(BUCKET_NAME, OBJECT_ULT_EJECUCION, "application/json").json()
        ultEjecucion["status"] = status
        ultEjecucion["error"] = errorMessage
        writeObject(BUCKET_NAME, OBJECT_ULT_EJECUCION, ultEjecucion)
    except Exception as e:
        raise Exception("UPDATE_ULT_EJECUCION_ERROR - " + str(e))


def writeObject(bucketName, objectName, content):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    output=""
    try:
        object = client.put_object(namespace, bucketName, objectName, json.dumps(content))
        output = "Success: Put object '" + objectName + "' in bucket '" + bucketName + "'"
    except Exception as e:
        raise Exception("WriteObject fn " + str(e))
    return { "state": output }

    