import csv
import os

from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    precision_recall_fscore_support,
)
from sklearn.model_selection import train_test_split
import xgboost as xgb


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "datasets", "sanmist_telemetry_synthetic.csv")
MODEL_PATH = os.path.join(BASE_DIR, "models", "xgboost_sei_classifier.json")
REPORT_WIDTH = 72


def load_dataset(csv_path: str) -> tuple[list[list[float]], list[int]]:
    """Load telemetry features and efficacy labels without pandas."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing dataset at {csv_path}.")

    features: list[list[float]] = []
    labels: list[int] = []

    with open(csv_path, mode="r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            features.append([
                float(row["ambient_temp_c"]),
                float(row["relative_humidity_pct"]),
                float(row["flow_rate_ml_min"]),
                float(row["cycle_duration_sec"]),
                float(row["room_volume_m3"]),
            ])
            labels.append(int(row["efficacy_class"]))

    return features, labels


def main() -> None:
    """Evaluate the exported native XGBoost classifier on the held-out test split."""
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Missing exported model at {MODEL_PATH}.")

    X, y = load_dataset(CSV_PATH)
    _, X_test, _, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = xgb.XGBClassifier()
    model.load_model(MODEL_PATH)
    y_pred = model.predict(X_test)

    total = len(y_test)
    correct = int(sum(int(yt) == int(yp) for yt, yp in zip(y_test, y_pred)))
    accuracy = accuracy_score(y_test, y_pred)

    precision, recall, f1, support = precision_recall_fscore_support(
        y_test, y_pred, labels=[0, 1, 2], zero_division=0
    )
    macro_p, macro_r, macro_f1, _ = precision_recall_fscore_support(
        y_test, y_pred, labels=[0, 1, 2], average="macro", zero_division=0
    )
    cm = confusion_matrix(y_test, y_pred, labels=[0, 1, 2])

    class_names = ["Fail (Class 0)", "Marginal (Class 1)", "Pass (Class 2)"]
    short_names = ["Fail (0)", "Marginal (1)", "Pass (2)"]

    sep_eq = "=" * REPORT_WIDTH
    sep_dash = "-" * REPORT_WIDTH

    print(sep_eq)
    print("SANMIST ML MODEL EVALUATION RESULTS".center(REPORT_WIDTH))
    print(sep_eq)
    print("Engine Mode: XGBoost Multiclass Classifier (Native JSON)")
    print(f"Evaluated Samples: {total} held-out test cycles ({correct}/{total} correct)")
    print(f"Overall Test Accuracy: {accuracy * 100:.2f}%")
    print(sep_eq)
    print()

    print("PER-CLASS METRICS")
    print(sep_dash)
    print(f"{'Class':<20} {'Precision':>12} {'Recall':>12} {'F1-Score':>12} {'Support':>12}")
    print(sep_dash)
    for idx, label in enumerate(class_names):
        print(
            f"{label:<20} "
            f"{precision[idx]:>12.4f} "
            f"{recall[idx]:>12.4f} "
            f"{f1[idx]:>12.4f} "
            f"{int(support[idx]):>12d}"
        )
    print(sep_dash)
    print(
        f"{'Macro Avg':<20} "
        f"{macro_p:>12.4f} "
        f"{macro_r:>12.4f} "
        f"{macro_f1:>12.4f} "
        f"{total:>12d}"
    )
    print(sep_dash)
    print()

    print("CONFUSION MATRIX (Rows: Actual | Cols: Predicted)")
    print(sep_dash)
    print(
        f"{'Actual \\ Predicted':<24} "
        f"{short_names[0]:>15} "
        f"{short_names[1]:>15} "
        f"{short_names[2]:>15}"
    )
    print(sep_dash)
    for row_idx, row_label in enumerate(short_names):
        print(
            f"{row_label:<24} "
            f"{int(cm[row_idx][0]):>15d} "
            f"{int(cm[row_idx][1]):>15d} "
            f"{int(cm[row_idx][2]):>15d}"
        )
    print(sep_eq)


if __name__ == "__main__":
    main()
