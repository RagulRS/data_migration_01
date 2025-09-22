# Backend/comparison_spec.py
import pandas as pd
from pathlib import Path

def compare_specifications(source_spec_file, target_spec_file, comparison_result_file,
                           source_spec_with_occurrence_file="./data/source_spec_with_occurrence.xlsx", target_spec_with_occurrence_file=None):
    """
    Compare form/field definitions between source_spec_file and target_spec_file.
    - source_spec_file, target_spec_file, comparison_result_file are Path-like or strings.
    - writes an xlsx with sheets Matched / Unmatched to comparison_result_file.
    - returns a small sample (first N rows) for UI preview.
    """
    source_spec_file = Path(source_spec_file)
    target_spec_file = Path(target_spec_file)
    comparison_result_file = Path(comparison_result_file)

    print("Comparing specifications:")
    print("  source:", source_spec_file)
    print("  target:", target_spec_file)

    # Read schedule sheets (to compute occurrence if present)
    try:
        source_schedule = pd.read_excel(source_spec_file, sheet_name='Schedule - Tree', engine='openpyxl')
    except Exception:
        source_schedule = pd.DataFrame()
    try:
        target_schedule = pd.read_excel(target_spec_file, sheet_name='Schedule - Tree', engine='openpyxl')
    except Exception:
        target_schedule = pd.DataFrame()

    def calculate_occurrences(df):
        if df.empty or 'Form Name' not in df.columns:
            return df
        df = df.copy()
        df['Occurrence'] = df.groupby('Form Name').cumcount() + 1
        return df

    source_schedule = calculate_occurrences(source_schedule)
    target_schedule = calculate_occurrences(target_schedule)

    # Read form definitions (these sheets must exist)
    source_df = pd.read_excel(source_spec_file, sheet_name='Form Definitions', engine='openpyxl')
    target_df = pd.read_excel(target_spec_file, sheet_name='Form Definitions', engine='openpyxl')

    matched_entries = []
    unmatched_entries = []
    exclude_values = ["_R_COPYSOURCE", "_R_COPYMOD", "", " "]

    for _, target_row in target_df.iterrows():
        # skip rows without label
        if pd.isna(target_row.get('Label')) or str(target_row.get('Label')).strip() == '':
            continue

        target_form_label = target_row.get('Form Label')
        target_label = target_row.get('Label')
        target_item_group = target_row.get('Item Group Name')
        target_item_name = target_row.get('Item Name')
        if target_item_name in exclude_values:
            continue

        # find matching rows in source by Form Label
        source_match = source_df[source_df['Form Label'] == target_form_label]
        if not source_match.empty:
            match_found = False
            for idx, source_row in source_match.iterrows():
                if source_row.get('Item Name') == target_item_name:
                    matched_entries.append({
                        'Form Name': target_row.get('Form Name'),
                        'Form Label': target_form_label,
                        'Item Group Name': target_item_group,
                        'Item Name': target_row.get('Item Name'),
                        'Item Label': target_label
                    })
                    source_df = source_df.drop(index=idx)
                    match_found = True
                    break
            if not match_found:
                unmatched_entries.append({
                    'Form Name': target_row.get('Form Name'),
                    'Form Label': target_form_label,
                    'Item Group Name': target_item_group,
                    'Item Name': target_row.get('Item Name'),
                    'Item Label': target_label
                })
        else:
            unmatched_entries.append({
                'Form Name': target_row.get('Form Name'),
                'Form Label': target_form_label,
                'Item Group Name': target_item_group,
                'Item Name': target_row.get('Item Name'),
                'Item Label': target_label
            })

    matched_df = pd.DataFrame(matched_entries)
    unmatched_df = pd.DataFrame(unmatched_entries)

    # write comparison result workbook
    with pd.ExcelWriter(comparison_result_file, engine='openpyxl') as writer:
        matched_df.to_excel(writer, sheet_name='Matched', index=False)
        unmatched_df.to_excel(writer, sheet_name='Unmatched', index=False)

    # optionally write schedules with occurrence (we keep as comment and skip if none provided)
    if source_spec_with_occurrence_file:
        pd.DataFrame(source_schedule).to_excel(source_spec_with_occurrence_file, sheet_name='Schedule Tree', index=False)
    if target_spec_with_occurrence_file:
        pd.DataFrame(target_schedule).to_excel(target_spec_with_occurrence_file, sheet_name='Schedule Tree', index=False)

    # Return small samples to UI (limit to 200 each)
    return {
        "matched_sample": matched_df.head(200).to_dict(orient="records"),
        "unmatched_sample": unmatched_df.head(200).to_dict(orient="records")
    }

# allow running standalone for debug
if __name__ == "__main__":
    # quick local test: paths need to be adapted
    pass
