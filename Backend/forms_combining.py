# Backend/forms_combaining.py
import os
import re
import pandas as pd
from datetime import datetime
from pathlib import Path

def combine_forms(csv_source_folder, comparison_result_file, target_spec_file,
                  source_spec_with_occurrence_file=None, target_spec_with_occurrence_file=None,
                  transformed_output_file=None):
    """
    csv_source_folder: folder containing CSVs (Path or string) - typically data/forms
    comparison_result_file: path to comparison_result.xlsx (sheet 'Matched' expected)
    target_spec_file: path to the target spec (for schedule/codelists)
    transformed_output_file: path where transformed CSV will be written
    """
    csv_source_folder = Path(csv_source_folder)
    comparison_result_file = Path(comparison_result_file)
    target_spec_file = Path(target_spec_file)
    transformed_output_file = Path(transformed_output_file)

    print("Combining form CSVs from:", csv_source_folder)

    # read helper spec files
    # source_spec_with_occurrence_file and target_spec_with_occurrence_file are optional
    # target spec used for form definitions / codelists
    form_def_df = pd.read_excel(target_spec_file, sheet_name='Form Definitions', engine='openpyxl')
    try:
        codelist_df = pd.read_excel(target_spec_file, sheet_name='Codelists', engine='openpyxl')
    except Exception:
        codelist_df = pd.DataFrame()
    try:
        unit_codelist_df = pd.read_excel(target_spec_file, sheet_name='Unit Codelists', engine='openpyxl')
    except Exception:
        unit_codelist_df = pd.DataFrame()

    # matched mapping produced by comparison_spec
    matched_df = pd.read_excel(comparison_result_file, sheet_name='Matched', engine='openpyxl')

    # For event order we attempt to read Schedule - Grid and take row 1 as you had before
    try:
        schedule_df = pd.read_excel(target_spec_file, sheet_name="Schedule - Grid", header=None, engine='openpyxl')
        event_order = schedule_df.iloc[1].dropna().tolist()
    except Exception:
        event_order = []

    transformed_data = []

    def get_choice_code(item_name, choice_label):
        if form_def_df is None or form_def_df.empty:
            return None
        item_row = form_def_df[form_def_df['Item Name'] == item_name]
        if not item_row.empty:
            data_type = item_row.iloc[0].get('Data Type', '')
            if 'Codelist' in str(data_type):
                code_list_name = item_row.iloc[0].get('Codelist')
                if not pd.isna(code_list_name) and not codelist_df.empty:
                    match_row = codelist_df[(codelist_df['Name'] == code_list_name) & (codelist_df['Choice Label'] == choice_label)]
                    if not match_row.empty:
                        return match_row.iloc[0].get('Choice Code')
            if 'Unit' in str(data_type):
                code_list_name = item_row.iloc[0].get('Unit Codelist')
                if not pd.isna(code_list_name) and not unit_codelist_df.empty:
                    match_row = unit_codelist_df[(unit_codelist_df['Name'] == code_list_name) & (unit_codelist_df['Choice Label'] == choice_label)]
                    if not match_row.empty:
                        return match_row.iloc[0].get('Choice Code')
            return "Not codelist"
        return None

    # iterate CSVs in source folder
    for filename in os.listdir(csv_source_folder):
        if filename.lower().endswith(".csv"):
            csv_path = csv_source_folder / filename
            csv_df = pd.read_csv(csv_path, dtype=str)  # read everything as str to avoid dtypes surprises
            # drop Item Group Sequence Number if present
            if 'Item Group Sequence Number' in csv_df.columns:
                dedup_df = csv_df.drop(columns=['Item Group Sequence Number']).drop_duplicates()
                csv_df = csv_df.loc[dedup_df.index].drop_duplicates()
            else:
                csv_df = csv_df.drop_duplicates()
            if csv_df.get('Form Label') is None or csv_df['Form Label'].dropna().empty:
                continue
            dominant_form_label = csv_df['Form Label'].mode()[0] if not csv_df['Form Label'].mode().empty else None
            matched_rows = matched_df[matched_df['Form Label'] == dominant_form_label].dropna()

            for col in csv_df.columns:
                m = re.search(r"\(([^)]+)\)$", col)
                if m:
                    full_item = m.group(1)
                    csv_item_name = full_item.split('.')[-1].strip().lower()
                    for _, matched_row in matched_rows.iterrows():
                        item_name = matched_row.get('Item Name', '')
                        item_group = matched_row.get('Item Group Name', '')
                        form_name = matched_row.get('Form Name', '')
                        if csv_item_name == str(item_name).strip().lower():
                            for _, csv_row in csv_df.iterrows():
                                event_label = csv_row.get('Event Label')
                                form_label = csv_row.get('Form Label')
                                # attempt to find occurrence via event/form (this logic expects presence of occurrence files; it's kept simple)
                                item_data = get_choice_code(item_name, csv_row.get(col))
                                if item_data == "Not codelist":
                                    item_data = csv_row.get(col, "")

                                # normalize item_data
                                try:
                                    parsed_date = datetime.strptime(str(item_data).strip(), "%d-%m-%Y")
                                    item_data = parsed_date.strftime("%Y-%m-%d")
                                except Exception:
                                    # leave as string trim
                                    if isinstance(item_data, float) and item_data.is_integer():
                                        item_data = str(int(item_data))
                                    else:
                                        item_data = str(item_data).strip()

                                # event details are simplified here
                                transformed_data.append({
                                    "Study": csv_row.get("Study", ""),
                                    "Study Country": csv_row.get("Study Country", ""),
                                    "Study Site": csv_row.get("Study Site", ""),
                                    "Subject": csv_row.get("Subject", ""),
                                    "Event Label": event_label,
                                    "Form Label": form_label,
                                    "Form Name": form_name,
                                    "Form Status": csv_row.get("Form Status", ""),
                                    "Item Group": item_group,
                                    "Item Name": item_name,
                                    "Item Data": item_data,
                                    "Event Date": csv_row.get("Event Date", "")
                                })

    final_df = pd.DataFrame(transformed_data)
    if event_order:
        try:
            final_df['Event Label'] = pd.Categorical(final_df['Event Label'], categories=event_order, ordered=True)
        except Exception:
            pass

    # sort and write output CSV
    grouped_sorted_df = final_df.sort_values(['Subject', 'Event Label']) if not final_df.empty else final_df
    grouped_sorted_df.to_csv(transformed_output_file, index=False)

    return {
        "rows": len(grouped_sorted_df),
        "sample": grouped_sorted_df.head(200).to_dict(orient="records")
    }

if __name__ == "__main__":
    pass
