import json
import boto3
from datetime import datetime, timedelta
from pymongo import MongoClient

def get_database():
    

    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = 'mongodb+srv://savalenciz:ng1zSFxngMDOwa9z@books-cc.naqfz.mongodb.net/bookstore?retryWrites=true&w=majority'

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['bookstore']

def handler(event, context):
    db = get_database()
    now = datetime.now()
    workloads = db.workloads.find(
        {
            "date": {"$lte": now}
        }
    ).limit(20)
    
    print(workloads)

    for workload in workloads:
        evaluations = db.evaluations.find(
            {
                "isEvaluated": False,
                "type": workload['type'],
                "date": {"$gte": workload['date'] - timedelta(days=workload['days'])},
                "date": {"$lte": workload['date']}
            }
        ).limit(workload['quantity'])
        evaluation_ids = [evaluation['_id'] for evaluation in evaluations]

        db.assignations.insert_one({
            "evaluations": evaluation_ids,
            "done": 0,
            "workload": workload['_id'],
            "user": workload['user'],
            "type": workload["type"],
            "date": now
        })

        db.workloads.update_one({"_id": workload['_id']}, {"$set": {"date": now + timedelta(days=workload['days'])}})

        db.evaluations.update_many({"_id": {"$in": evaluation_ids}}, {"$set": {"isEvaluated": True}})
    
        notification = "Workload {0} had been assigned to you".format(workload['name'])
        client = boto3.client('sns')
        
        response = client.publish (
            TargetArn = 'arn:aws:sns:us-east-1:821299781912:WorkloadsAssignation',
            Message = json.dumps({'default': notification}),
            MessageStructure = 'json'
            )
    
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }

handler(None,None)