# Backend/app.py
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
from pathlib import Path
import os, traceback

# Make project paths
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
FORMS_DIR = DATA_DIR / "forms"

DATA_DIR.mkdir(parents=True, exist_ok=True)
FORMS_DIR.mkdir(parents=True, exist_ok=True)

# import the functions from the modified modules
import comparison_spec as comp_mod
import forms_combining as forms_mod
import vault_migration as vault_mod

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100MB max upload

@app.route("/api/migrate", methods=["POST"])
def api_migrate():
    try:
        # read form fields
        study_id = (request.form.get("studyId") or "").strip()
        site_id = (request.form.get("siteId") or "").strip()
        site_country = (request.form.get("siteCountry") or "").strip()
        subjects = (request.form.get("subjects") or "").strip()

        if not study_id or not site_id or not site_country:
            return jsonify({"error": "Please provide studyId, siteId and siteCountry"}), 400

        # parse subject mappings
        # expected format: OLD1:NEW1,OLD2:NEW2  OR OLD1,OLD2 (if no new provided)
        subject_mappings = []
        if subjects:
            for part in [p.strip() for p in subjects.split(",") if p.strip()]:
                if ":" in part:
                    old, new = [x.strip() for x in part.split(":", 1)]
                    subject_mappings.append((old, new))
                else:
                    subject_mappings.append((part, None))

        # file upload
        if "targetSpec" not in request.files:
            return jsonify({"error": "targetSpec file missing"}), 400
        f = request.files["targetSpec"]
        filename = secure_filename(f.filename) or "target_spec.xlsx"
        target_spec_path = DATA_DIR / filename
        f.save(str(target_spec_path))

        # required input: source_spec.xlsx must exist in data folder
        source_spec_path = DATA_DIR / "source_spec.xlsx"
        if not source_spec_path.exists():
            return jsonify({
                "error": f"Source spec not found at {source_spec_path}. Place your source_spec.xlsx inside the data folder."
            }), 400

        # output paths (all inside data/)
        comparison_result_file = DATA_DIR / "comparison_result.xlsx"
        # per your request: comment placeholders for these (we set to None now)
        source_spec_with_occurrence_file = None
        target_spec_with_occurrence_file = None

        transformed_output_file = DATA_DIR / "transformed_output.csv"
        failed_items_output_file = DATA_DIR / "failed_items_output.txt"
        output_log_file = DATA_DIR / "output_log.txt"

        # 1) Compare specs
        compare_res = comp_mod.compare_specifications(
            source_spec_file=source_spec_path,
            target_spec_file=target_spec_path,
            comparison_result_file=comparison_result_file,
            source_spec_with_occurrence_file=source_spec_with_occurrence_file,
            target_spec_with_occurrence_file=target_spec_with_occurrence_file
        )

        # 2) Combine forms / transform
        combine_res = forms_mod.combine_forms(
            csv_source_folder=FORMS_DIR,
            comparison_result_file=comparison_result_file,
            target_spec_file=target_spec_path,
            source_spec_with_occurrence_file=source_spec_with_occurrence_file,
            target_spec_with_occurrence_file=target_spec_with_occurrence_file,
            transformed_output_file=transformed_output_file
        )

        # 3) Migrate to Veeva Vault — only if environment variables are set
        vault_config = {
            "VAULT_DNS": os.getenv("VAULT_DNS"),
            "API_VERSION": os.getenv("VAULT_API_VERSION", "v23.2"),
            "USERNAME": os.getenv("VAULT_USERNAME"),
            "PASSWORD": os.getenv("VAULT_PASSWORD")
        }

        if not (vault_config["VAULT_DNS"] and vault_config["USERNAME"] and vault_config["PASSWORD"]):
            vault_res = {"skipped": True, "message": "Vault credentials not provided. Set VAULT_DNS, VAULT_USERNAME and VAULT_PASSWORD env vars to enable migration."}
        else:
            old_subj_list = [s[0] for s in subject_mappings]
            new_subj_list = [(s[1] if s[1] else s[0]) for s in subject_mappings]
            vault_res = vault_mod.migrate_to_vault(
                transformed_output_file=transformed_output_file,
                study_name=study_id,
                site_number=site_id,
                study_country=site_country,
                old_subj_list=old_subj_list,
                new_subj_list=new_subj_list,
                data_dir=DATA_DIR,
                vault_config=vault_config
            )

        resp = {
            "comparison": {
                "matched_sample": compare_res.get("matched_sample", []),
                "unmatched_sample": compare_res.get("unmatched_sample", [])
            },
            "combine": {
                "rows": combine_res.get("rows", 0),
                "sample": combine_res.get("sample", [])
            },
            "vault": vault_res
        }
        return jsonify(resp)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
