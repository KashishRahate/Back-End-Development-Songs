from . import app
import os
import json
import pymongo
from flask import jsonify, request, make_response, abort, url_for  # noqa: F401
from pymongo import MongoClient
from bson import json_util
from pymongo.errors import OperationFailure
from bson.objectid import ObjectId
import sys

# Load songs data from JSON file
SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
json_url = os.path.join(SITE_ROOT, "data", "songs.json")
songs_list: list = json.load(open(json_url))

# MongoDB connection setup
mongodb_service = os.environ.get('MONGODB_SERVICE')
mongodb_username = os.environ.get('MONGODB_USERNAME')
mongodb_password = os.environ.get('MONGODB_PASSWORD')
mongodb_port = os.environ.get('MONGODB_PORT')

print(f'The value of MONGODB_SERVICE is: {mongodb_service}')

if mongodb_service is None:
    app.logger.error('Missing MongoDB server in the MONGODB_SERVICE variable')
    sys.exit(1)

if mongodb_username and mongodb_password:
    url = f"mongodb://{mongodb_username}:{mongodb_password}@{mongodb_service}"
else:
    url = f"mongodb://{mongodb_service}"

print(f"Connecting to URL: {url}")

try:
    client = MongoClient(url)
except OperationFailure as e:
    app.logger.error(f"Authentication error: {str(e)}")
    sys.exit(1)

# Initialize the database and collection
db = client.songs
db.songs.drop()
db.songs.insert_many(songs_list)

def parse_json(data):
    """Helper function to parse MongoDB data to JSON."""
    return json.loads(json_util.dumps(data))

######################################################################
# Endpoints
######################################################################

# Health Endpoint
@app.route("/health", methods=["GET"])
def health():
    """Endpoint to check the health of the server."""
    return jsonify({"status": "OK"})

# Count Endpoint
@app.route("/count", methods=["GET"])
def count():
    """Endpoint to count the number of documents in the 'songs' collection."""
    song_count = db.songs.count_documents({})
    return jsonify({"count": song_count})

# GET /song Endpoint
@app.route("/song", methods=["GET"])
def songs():
    """Endpoint to retrieve all songs."""
    all_songs = list(db.songs.find({}))  # Retrieve all documents in the 'songs' collection
    return jsonify({"songs": parse_json(all_songs)}), 200

# GET /song/<id> Endpoint
@app.route("/song/<int:id>", methods=["GET"])
def get_song_by_id(id):
    """Endpoint to retrieve a song by its id."""
    song = db.songs.find_one({"id": id})  # Find a song by id
    if song:
        return jsonify(parse_json(song)), 200
    else:
        return jsonify({"message": "song with id not found"}), 404

# POST /song Endpoint
@app.route("/song", methods=["POST"])
def create_song():
    """Endpoint to create a new song."""
    song_data = request.get_json()
    existing_song = db.songs.find_one({"id": song_data["id"]})
    if existing_song:
        return jsonify({"Message": f"song with id {song_data['id']} already present"}), 302
    
    result = db.songs.insert_one(song_data)
    return jsonify({"inserted id": {"$oid": str(result.inserted_id)}}), 201

# PUT /song/<id> Endpoint
@app.route("/song/<int:id>", methods=["PUT"])
def update_song(id):
    """Endpoint to update an existing song."""
    song_data = request.get_json()
    existing_song = db.songs.find_one({"id": id})
    
    if existing_song:
        update_result = db.songs.update_one({"id": id}, {"$set": song_data})
        if update_result.modified_count > 0:
            updated_song = db.songs.find_one({"id": id})
            return jsonify(parse_json(updated_song)), 200
        else:
            return jsonify({"message": "song found, but nothing updated"}), 200
    else:
        return jsonify({"message": "song not found"}), 404

# DELETE /song/<id> Endpoint
@app.route("/song/<int:id>", methods=["DELETE"])
def delete_song(id):
    """Endpoint to delete a song by its id."""
    delete_result = db.songs.delete_one({"id": id})
    if delete_result.deleted_count == 0:
        return jsonify({"message": "song not found"}), 404
    return '', 204  # No content

