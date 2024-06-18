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

def handler(ctx, data: io.BytesIO=None):
    try:
        body = json.loads(data.getvalue())
        bucketName = body["bucketName"]
        objectName = body["objectName"]
        #dateFrom = body["dateFrom"]
    except Exception:
        error = 'Input a JSON object in the format: \'{"bucketName": "<bucket name>"}, "objectName": "<object name>", "dateFrom" : "<date from>"}\' '
        raise Exception(error)
    #resp = readGravimetricoData(bucketName, objectName, dateFrom)
    resp = pruebaMerge(bucketName, objectName, bucketName, "erp-data.json")
    #resp = readGravimetricoData(bucketName, objectName, "2023/02/21 13:36:36")
    return response.Response(
        ctx,
        #response_data=json.dumps(resp),
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
        raise Exception("Ocurrio un error al llamar a la API")
    return response


def readGravimetricoData(bucketName, objectName, dateFrom):
    print("Running readGravimetricoData function")
    try:
        content = getObject(bucketName, objectName)
        dateFromObject = datetime.strptime(dateFrom, "%Y/%m/%d %H:%M:%S")
        lines = content.text.split("\r\n")
        consumos = {}
        for line in lines:
            [date, item, intake] = line.split(",")
            dateObject = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
            if(dateObject >= dateFromObject):
                if(item in consumos):
                    consumos[item] += float(intake)
                else:
                    consumos[item] = float(intake)
    except Exception as e:
        consumos = {"Ocurrio un error" : e.message}
    return consumos

def pruebaMerge(bucketNameGrav, objectNameGrav, bucketNameErp, objectNameErp):
    try:
        consumosGrav = readGravimetricoData(bucketNameGrav, objectNameGrav, "2023/02/21 13:36:36")
        consumosErp = getObject(bucketNameErp, objectNameErp, "application/json").json()["consumos"]
        listaResultado = []
        for consumo in consumosErp:
            resultado = {}
            #TODO: Chequear que pasa si no existe el consumo del erp en el gravimetrico
            resultado["Item"] = consumo["item"]
            resultado["ConsumoProporcional"] = (consumosGrav[consumo["item"]] * int(consumo["porcentajeIncidencia"])) / 100
            resultado["OrdenDeTrabajo"] = consumo["ordenProd"]
            resultado["Gravimetrico"] = consumosGrav[consumo["item"]]
            resultado["PorcentajeIncidencia"] = consumo["porcentajeIncidencia"]
            listaResultado.append(resultado)
        return listaResultado
        #response = {"Tipo de dato: " : responseL}
        #response = readJson(consumosErp)
    except Exception as e:
        response = {"Ocurrio un error" : "da"}
    return response


def writeObject(bucketName, objectName, content):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    output=""
    try:
        object = client.put_object(namespace, bucketName, objectName, json.dumps(content))
        output = "Success: Put object '" + objectName + "' in bucket '" + bucketName + "'"
    except Exception as e:
        output = "Failed: " + str(e.message)
    return { "state": output }

    