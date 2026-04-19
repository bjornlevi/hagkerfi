"""Flask web app for the Hagkerfi economic simulation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, jsonify, render_template, request
from simulation import SimulationEngine

app = Flask(__name__, template_folder="templates")
engine = SimulationEngine()


@app.route("/")
def index():
    return render_template("index.html")


# ── Active simulation ─────────────────────────────────────────────────────────

@app.route("/api/init", methods=["POST"])
def init():
    try:
        return jsonify(engine.initialize())
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/advance", methods=["POST"])
def advance():
    try:
        return jsonify(engine.advance())
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/snapshot/<int:month>")
def snapshot(month: int):
    data = engine.get_snapshot(month)
    if data is None:
        return jsonify({"error": "month not available"}), 404
    return jsonify(data)


# ── Persistence ───────────────────────────────────────────────────────────────

@app.route("/api/simulations", methods=["GET"])
def list_simulations():
    return jsonify(SimulationEngine.list_saved())


@app.route("/api/simulations/<name>", methods=["POST"])
def save_simulation(name: str):
    try:
        meta = engine.save(name)
        return jsonify(meta)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/simulations/<name>/load", methods=["POST"])
def load_simulation(name: str):
    try:
        return jsonify(engine.load(name))
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/simulations/<name>", methods=["DELETE"])
def delete_simulation(name: str):
    try:
        SimulationEngine.delete_saved(name)
        return jsonify({"ok": True})
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Compare ───────────────────────────────────────────────────────────────────

@app.route("/api/compare")
def compare():
    names = [n.strip() for n in request.args.get("names", "").split(",") if n.strip()]
    if not names:
        return jsonify({"error": "provide ?names=a,b"}), 400
    result = []
    for name in names:
        try:
            result.append(SimulationEngine.timeseries_for(name))
        except Exception as exc:
            result.append({"name": name, "error": str(exc)})
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
