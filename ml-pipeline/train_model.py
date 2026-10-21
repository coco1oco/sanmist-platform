import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import xgboost as xgb
import joblib

os.makedirs("models", exist_ok=True)

# 1. Load dataset
csv_path = os.path.join("datasets", "sanmist_telemetry_synthetic.csv")
df = pd.read_csv(csv_path)

features = ['ambient_temp_c', 'relative_humidity_pct', 'flow_rate_ml_min', 'cycle_duration_sec', 'room_volume_m3']
X = df[features]
y = df['efficacy_class']

# 2. Train/Test Split (80/20)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# 3. Train XGBoost Classifier
model = xgb.XGBClassifier(
    n_estimators=100,
    max_depth=4,
    learning_rate=0.08,
    objective='multi:softprob',
    num_class=3,
    random_state=42
)
model.fit(X_train, y_train)

# 4. Evaluation
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f"\nModel Accuracy: {accuracy * 100:.2f}%\n")
print(classification_report(y_test, y_pred, target_names=['Fail (0)', 'Marginal (1)', 'Pass (2)']))

# 5. Save model artifact
model_path = os.path.join("models", "xgboost_sei_classifier.pkl")
joblib.dump(model, model_path)
print(f"Saved trained model artifact to: {model_path}")