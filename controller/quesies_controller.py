from datetime import datetime

from flask import jsonify, request, Blueprint
from service.queries_service import get_total_accidents_by_area, get_accidents_by_period_and_area, \
    get_total_accidents_by_area_and_period

queries_blueprint = Blueprint('queries', __name__)

@queries_blueprint.route('/accidents/area', methods=['GET'])
def total_accidents_area():
    area = request.args.get('area')
    if not area:
        return jsonify({"status": "error", "message": "Missing 'area' parameter"}), 400

    try:
        total_accidents = get_total_accidents_by_area(area)
        return jsonify({"status": "success", "area": area, "total_accidents": total_accidents}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@queries_blueprint.route('/accidents/area/period', methods=['GET'])
def total_accidents_area_period():
    params = request.args.to_dict()
    required_keys = ['area', 'period', 'date']

    if not all(k in params for k in required_keys):
        return jsonify({"status": "error", "message": "Missing parameters"}), 400

    try:
        total_accidents = get_total_accidents_by_area_and_period(params['area'], params['date'], params['period'])
        return jsonify({"status": "success", "area": params['area'], "period": params['period'], "date": params['date'],
                        "total_accidents": total_accidents}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
# GET /accidents/area/period?area=123&period=month&date=2024-10-10
