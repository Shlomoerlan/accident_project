from datetime import datetime

from flask import jsonify, request, Blueprint
from service.queries_service import get_total_accidents_by_area, get_accidents_by_period_and_area

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
    area = request.args.get('area')
    period = request.args.get('period')
    date_str = request.args.get('date')

    if not area or not period or not date_str:
        return jsonify({"status": "failure", "message": "Missing 'area', 'period', or 'date' parameter"}), 400

    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        total_accidents = get_accidents_by_period_and_area(area, date, period)
        return jsonify({"status": "success", "area": area, "period": period, "date": date_str, "total_accidents": total_accidents}), 200
    except KeyError:
        return jsonify({"status": "failure", "message": "Invalid period parameter"}), 400
    except Exception as e:
        return jsonify({"status": "failure", "message": "Failed to fetch data", "error": str(e)}), 500

# GET /accidents/area/period?area=123&period=month&date=2024-10-10
