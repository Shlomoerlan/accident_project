from flask import Flask

from controller.init_controller import initiate_blueprint
from controller.quesies_controller import queries_blueprint
from database.connect import daily_collection, monthly_collection

app = Flask(__name__)

if __name__ == '__main__':
    app.register_blueprint(initiate_blueprint, url_prefix="/api/initiate")
    app.register_blueprint(queries_blueprint, url_prefix="/api/queries")
    print(list(daily_collection.find()))
    app.run(debug=True)





