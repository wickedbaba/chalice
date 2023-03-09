from chalice import Chalice
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

import boto3
from boto3.dynamodb.conditions import Key,Attr


# MongoDB Functions


client = MongoClient(MONGO_URI)

# db = client['car_portal']
# table = db.create_collection('customer_details')
table = client.car_portal.customer_details

app = Chalice(app_name='chalice-car-system')

@app.route('/')
def index():
    return {'hello': 'world', 'methods': 'CRUD operations using GET and POST ', 'endpoints': ['/customer','/car']}

#  to get one user
@app.route('/customer/getData/one', methods=['GET'])
def get_hobbies():
    data = app.current_request.json_body
    id_to_find = data['license_number']

    # table.find()
    required_customer_data = table.find({'license_number':id_to_find})
    results = []
    for data in required_customer_data:
        output = {}
        output['license_number'] = data['license_number']
        output['name'] = data['name']
        output['age'] = data['age']
        output['address'] = data['address']
        output['phone_number'] = data['phone_number']
        output['email'] = data['email']
        output['car_number'] = data['car_number']
        results.append(output)
    return results

#  to get all users
@app.route('/customer/getData/all', methods=['GET'])
def get_hobbies():
    customer_data = table.find({})
    results = []
    for data in customer_data:
        output = {}
        output['license_number'] = data['license_number']
        output['name'] = data['name']
        output['age'] = data['age']
        output['address'] = data['address']
        output['phone_number'] = data['phone_number']
        output['email'] = data['email']
        output['car_number'] = data['car_number']
        results.append(output)
    return results

#  to insert one user
@app.route('/customer/insertData/Single', methods =['POST'])
def add_single_customer():
    try :
        data = app.current_request.json_body
        newValue = {}
        newValue['license_number'] = data['license_number']
        newValue['name'] = data['name']
        newValue['age'] = data['age']
        newValue['address'] = data['address']
        newValue['phone_number'] = data['phone_number']
        newValue['email'] = data['email']
        newValue['car_number'] = data['car_number']
        table.insert_one(newValue)
        return {"Successfully inserted":200}
    except Exception as X:
        # print(app.current_request.json_body)
        return {'message':str(X), }
    
#  insert multiple user
@app.route('/customer/insertData/multiple',methods=['POST'])
def add_multiple_customer():
    try:
        data = app.current_request.json_body
        # print(data)
        result = []
        for single_data in data:

            newValue ={}
            newValue['license_number'] = single_data['license_number']
            newValue['name'] = single_data['name']
            newValue['age'] = single_data['age']
            newValue['address'] = single_data['address']
            newValue['phone_number'] = single_data['phone_number']
            newValue['email'] = single_data['email']
            newValue['car_number'] = single_data['car_number']
            result.append(newValue)

        table.insert_many(result)
        return {"Successfully inserted":200}
    except Exception as X:
        print(X)

#  update single user
@app.route('/customer/updateData/single', methods=['POST'])
def update_single_customer():
    try:
        data = app.current_request.json_body

        id_to_find = data['license_number']        

        required_customer_data = table.find({'license_number':id_to_find})

        for element in required_customer_data:
            insertion_list = {}
            insertion_list['name'] = (data['name']  if data['name'] != '' else element['name'])
            insertion_list['age'] = data['age']  if data['age'] != '' else element['age']
            insertion_list['address'] = (data['address']  if data['address'] != '' else element['address'])
            insertion_list['phone_number'] = (data['phone_number']  if data['phone_number'] != '' else element['phone_number'])
            insertion_list['email'] = (data['email']  if data['email'] != '' else element['email'])
            insertion_list['car_number'] = (data['car_number']  if data['car_number'] != '' else element['car_number'])

            document_to_update = {'_id':ObjectId(element['_id'])}
            set_field = {"$set":  {  'name': insertion_list['name'],'age':insertion_list['age'],'address':insertion_list['address'],'phone_number':insertion_list['phone_number'],'email':insertion_list['email'],'car_number':insertion_list['car_number']}}

            result = table.update_one(document_to_update,set_field)
        
        return  {"Successfully inserted":200, "Match Count ": result.matched_count, "Modified Count":result.modified_count}
    
    except Exception as X:
        print(X)

#  update multiple user
@app.route('/customer/updateData/multiple', methods=['POST'])
def update_single_customer():
    try:
        data = app.current_request.json_body

        for single_data in data:
            id_to_find = single_data['license_number']        

            required_customer_data = table.find({'license_number':id_to_find})

            for element in required_customer_data:
                insertion_list = {}
                insertion_list['name'] = (single_data['name']  if single_data['name'] != '' else element['name'])
                insertion_list['age'] = single_data['age']  if single_data['age'] != '' else element['age']
                insertion_list['address'] = (single_data['address']  if single_data['address'] != '' else element['address'])
                insertion_list['phone_number'] = (single_data['phone_number']  if single_data['phone_number'] != '' else element['phone_number'])
                insertion_list['email'] = (single_data['email']  if single_data['email'] != '' else element['email'])
                insertion_list['car_number'] = (single_data['car_number']  if single_data['car_number'] != '' else element['car_number'])

                document_to_update = {'_id':ObjectId(element['_id'])}
                set_field = {"$set":  {  'name': insertion_list['name'],'age':insertion_list['age'],'address':insertion_list['address'],'phone_number':insertion_list['phone_number'],'email':insertion_list['email'],'car_number':insertion_list['car_number']}}

                result = table.update_one(document_to_update,set_field)
        
        return  {"Successfully inserted":200}
    
    except Exception as X:
        print(X)


#  delete single user
@app.route('/customer/deleteData/single', methods=['POST'])
def update_single_customer():
    try:
        data = app.current_request.json_body

        id_to_find = data['license_number']        

        result = table.delete_one({'license_number':id_to_find})
        
        return  {"Successfully deleted":200, "Deleted Count": result.deleted_count}
    
    except Exception as X:
        print(X)

#  delete multiple user
@app.route('/customer/deleteData/multiple', methods=['POST'])
def update_single_customer():
    try:
        data = app.current_request.json_body

        positive_deletes  = 0
        negative_detetes = 0

        for single_data in data:
            id_to_find = single_data['license_number']        
            result = table.delete_one({'license_number':id_to_find})

            if result.deleted_count > 0 :
                positive_deletes +=1
            else:
                negative_detetes +=1
        
        return  {"Successfully deleted":200, "Deleted Count": positive_deletes, "Not Deleted" : negative_detetes}
    
    except Exception as X:
        print(X)


# DynamoDB Functions 

def get_app_db():
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table('car-details')
    return table

#  create
@app.route('/car/insertData/single',methods= ['POST'])
def add_single_car():
    try:
        data = app.current_request.json_body
        get_app_db().put_item(Item={
            'car-number':data['car-number'],
            'license-number':data['license-number'],
            'car-brand':data['car-brand'],
            'owner-name':data['owner-name'],
            'date-of-purchase':data['date-of-purchase']
        })

        return {'message': 'ok - CREATED', 'status': 201}
    
    except Exception as X:
        return {'Sadly its this error' : X}
    
# Read
@app.route('/car/getData/single', methods=['GET'])
def get_book():
    try :
        data = app.current_request.json_body
        print(data)
        id = data['car-number']

        response = get_app_db().query(
            KeyConditionExpression = Key("car-number").eq(id)
        )

        result = response.get('Items',None)
        return {'Response' :result }

    except Exception as X:

        return {'Sadly, it is this error':X}

# Update
@app.route('/car/updateData/single', methods=['POST'])
def update_book():
    try:
        data = app.current_request.json_body

        get_app_db().update_item(Key={
            'car-number':data['car-number'],
            'license-number':data['license-number'],
        },
            ReturnValues="UPDATED_NEW"
        )
        return {'message': 'ok - UPDATED', 'status': 201}
    except Exception as e:
        return {'message': str(e)}