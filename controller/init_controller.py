from flask import Blueprint, jsonify
from repository.csv_repository import insert_data_to_mongo

initiate_blueprint = Blueprint('initiate', __name__)

@initiate_blueprint.route('/', methods=['POST'])
def initialize():
    try:
        insert_data_to_mongo()
        return jsonify({'status': 'success', 'message': 'Database initialized and indexes created'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500