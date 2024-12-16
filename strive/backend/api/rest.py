import base64

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

from core.system_processor import get_cycles_and_dag_paths, set_show_dag, generate_system_graph_image
from parser.system_parser import create_graph_from_system
from resources.system_examples import example_1

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/dsl', methods=['POST'])
@cross_origin()
def process_text():
    text = request.get_json()['text']

    system = example_1()

    graph = create_graph_from_system(system)

    png_bytes = generate_system_graph_image(graph)
    encoded_png = base64.b64encode(png_bytes).decode('utf-8')

    print(len(encoded_png))
    set_show_dag(False)
    cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)

    print("REST")
    print(cycles, paths)
    return jsonify({'image': encoded_png, 'cycles': str(cycles), 'paths': str(paths)})


if __name__ == '__main__':
    app.run(debug=True)
