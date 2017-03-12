from flask import Blueprint, send_from_directory

static_blueprint = Blueprint('static', __name__, static_folder='static')

@static_blueprint.route('/', defaults={'page': 'index'}, )
@static_blueprint.route('/<page>')
def show(page):
    print(static_blueprint.root_path)
    return send_from_directory('', page)
