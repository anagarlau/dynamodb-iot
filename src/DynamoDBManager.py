"""
Purpose: This class contains all the operation to perform on the DynamoDB table

"""
from s2.S2Manager import S2Manager
from model.PutPointInput import PutPointInput


class DynamoDBManager:
    

    def __init__(self, config):
        self.config = config
    

    def put_Point(self, putPointInput):
        """
        The dict in Item put_item call, should contains a dict with string as a key and a string as a value: {"N": "123"}
        """
        geohash = S2Manager().generateGeohash(putPointInput.GeoPoint)
        hashKey = S2Manager().generateHashKey(geohash, self.config.hashKeyLength)
        response = ""
        item=self.dict_to_item(putPointInput.ExtraFields)  # prepare the non key attributes first

        item[self.config.hashKeyAttributeName] ={"N": str(hashKey)}
        item[self.config.rangeKeyAttributeName] ={"S": putPointInput.RangeKeyValue}
        item[self.config.geohashAttributeName] ={'N': str(geohash)}
        item[self.config.geoJsonAttributeName] ={"S": "{},{}".format(putPointInput.GeoPoint.latitude,putPointInput.GeoPoint.longitude)}

        try:
            response = self.config.dynamoDBClient.put_item(
                TableName=self.config.tableName,
                Item=item 
            )
        except Exception as e:
            print("The following error occured during the item insertion :{}".format(e))
            response = "Error"
        return response

    def get_Point(self, getPointInput):
        """
        The dict in Key get_item call, should contains a dict with string as a key and a string as a value: {"N": "123"}
        """
        geohash = S2Manager().generateGeohash(getPointInput.GeoPoint)
        hashKey = S2Manager().generateHashKey(geohash, self.config.hashKeyLength)
        response = ""
        try:
            response = self.config.dynamoDBClient.get_item(
                TableName=self.config.tableName,
                Key={
                    self.config.hashKeyAttributeName: {"N": str(hashKey)},
                    self.config.rangeKeyAttributeName: {
                        "S": getPointInput.RangeKeyValue}
                }
            )
        except Exception as e:
            print("The following error occured during the item retrieval :{}".format(e))
            response = "Error"
        return response

    ''' 
    Helper function taken from here : https://gist.github.com/JamieCressey/a3a75a397db092d7a70bbe876a6fb817
    Take a dict as input and return dynamodb put_item item as result
    '''
    def dict_to_item(self,raw):
        if type(raw) is dict:
            resp = {}
            for k,v in raw.items():
                if type(v) is str:
                    resp[k] = {
                        'S': v
                    }
                elif type(v) is int:
                    resp[k] = {
                        'N': str(v)
                    }
                elif type(v) is dict:
                    resp[k] = {
                        'M': self.dict_to_item(v)
                    }
                elif type(v) is list:
                    resp[k] = []
                    for i in v:
                        resp[k].append(self.dict_to_item(i))
                        
            return resp
        elif type(raw) is str:
            return {
                'S': raw
            }
        elif type(raw) is int:
            return {
                'N': str(raw)
            }
