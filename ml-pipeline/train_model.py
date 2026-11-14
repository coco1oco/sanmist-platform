import csv
import os
import joblib
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split
import xgboost as xgb

os.makedirs("models", exist_ok=True)
csv_path = os.path.join("datasets", "sanmist_telemetry_synthetic.csv")

if not os.path.exists(csv_path):
    raise FileNotFoundError(f"Missing dataset at {csv_path}. Run generate_dataset.py first.")

# Read features using native csv library
X = []
y = []

with open(csv_path, mode="r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        X.append([
            float(row["ambient_temp_c"]),
            float(row["relative_humidity_pct"]),
            float(row["flow_rate_ml_min"]),
            float(row["cycle_duration_sec"]),
            float(row["room_volume_m3"])
        ])
        y.append(int(row["efficacy_class"]))

# 80/20 Stratified Train/Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Train XGBoost Multiclass Classifier
model = xgb.XGBClassifier(
    n_estimators=100,
    max_depth=4,
    learning_rate=0.08,
    objective="multi:softprob",
    num_class=3,
    eval_metric="mlogloss",
    random_state=42
)
model.fit(X_train, y_train)

# Evaluation
y_pred = model.predict(X_test)
acc = accuracy_score(y_test, y_pred)

print("=" * 60)
print(f"MistSan XGBoost SEI Classifier - Test Accuracy: {acc * 100:.2f}%")
print("=" * 60)
print(classification_report(
    y_test, 
    y_pred, 
    labels=[0, 1, 2], 
    target_names=["Fail (Class 0)", "Marginal (Class 1)", "Pass (Class 2)"]
))

# Save artifacts
model_pkl = os.path.join("models", "xgboost_sei_classifier.pkl")
joblib.dump(model, model_pkl)
print(f"[EXPORT] Pickled model saved to: {model_pkl}")

model_json = os.path.join("models", "xgboost_sei_classifier.json")
model.save_model(model_json)
print(f"[EXPORT] Native JSON model saved to: {model_json}")