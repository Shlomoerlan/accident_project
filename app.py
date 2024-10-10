from flask import Flask

from controller.init_controller import initiate_blueprint
from database.connect import daily_collection, monthly_collection

app = Flask(__name__)

if __name__ == '__main__':
    app.register_blueprint(initiate_blueprint, url_prefix="/api/initiate")
    app.run(debug=True)





