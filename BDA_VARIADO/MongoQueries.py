from pymongo import MongoClient

class MongoQueries:
    def __init__(self, database_name, port, username, password, host="localhost"):
        mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/"
        
        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]
        
    def get_team_and_person_count(self):
    
        result = self.db.works_in_team.aggregate([
            {
                "$lookup": {
                    "from": "teams",  
                    "localField": "team_id", 
                    "foreignField": "team_id",  
                    "as": "team_info"  
                }
            },
            {
                "$unwind": "$team_info"  
            },
            {
                "$group": {
                    "_id": "$team_info.name",  
                    "num_personas": {"$sum": 1}  
                }
            },
            {
                "$project": {
                    "_id": 0,  
                    "team_name": "$_id", 
                    "num_personas": 1  
                }
            }
        ])
        return list(result)
    def get_persons_in_team(self, team_name):
       
        result = self.db.works_in_team.aggregate([
            {
                "$lookup": {
                    "from": "teams",  
                    "localField": "team_id",  
                    "foreignField": "team_id",  
                    "as": "team_info"  
                }
            },
            {
                "$unwind": "$team_info" 
            },
            {
                "$match": {
                    "team_info.name": team_name 
                }
            },
            {
                "$project": {
                    "_id": 0,  
                    "person_id": 1,  
                    "rol": 1  
                }
            }
        ])

        return list(result)
    def get_teams_and_project_count(self):
        
        result = self.db.teams.aggregate([
            {
                "$group": {
                    "_id": "$team_id", 
                    "team_name": {"$first": "$name"}, 
                    "unique_projects": {"$addToSet": "$project_id"}  
                }
            },
            {
                "$project": {
                    "_id": 0,  
                    "team_name": 1,  
                    "num_projects": {"$size": "$unique_projects"}  
                }
            }
        ])

        return list(result)
