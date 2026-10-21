import os
import numpy as np
import pandas as pd

# Gumawa ng datasets directory kung wala pa
os.makedirs("datasets", exist_ok=True)

np.random.seed(42)
n_samples = 2000

# Feature simulation para sa commercial kitchen BOH conditions
ambient_temp = np.random.uniform(22.0, 38.0, n_samples)       # Deg C (DHT22)
relative_humidity = np.random.uniform(45.0, 92.0, n_samples)  # % RH (DHT22)
flow_rate = np.random.uniform(15.0, 48.0, n_samples)          # mL/min (YF-S401 proxy)
cycle_duration = np.random.uniform(60, 240, n_samples)        # Seconds
room_volume = np.random.uniform(15.0, 45.0, n_samples)        # m^3

# Saturation Efficiency Index (SEI) empirical calculation
# Mist volume delivered per cubic meter adjusted for ambient humidity deficit
sei = ((flow_rate * (cycle_duration / 60.0)) / room_volume) * (1.0 - (relative_humidity / 150.0))

# 0 = Fail (Under-saturated), 1 = Marginal, 2 = Pass (Effective Dry-Fog)
labels = []
for score in sei:
    if score >= 4.5:
        labels.append(2)  # Pass
    elif score >= 2.8:
        labels.append(1)  # Marginal
    else:
        labels.append(0)  # Fail

df = pd.DataFrame({
    'ambient_temp_c': np.round(ambient_temp, 2),
    'relative_humidity_pct': np.round(relative_humidity, 2),
    'flow_rate_ml_min': np.round(flow_rate, 2),
    'cycle_duration_sec': np.round(cycle_duration, 1),
    'room_volume_m3': np.round(room_volume, 1),
    'sei_score': np.round(sei, 3),
    'efficacy_class': labels
})

output_file = os.path.join("datasets", "sanmist_telemetry_synthetic.csv")
df.to_csv(output_file, index=False)
print(f"Generated {n_samples} telemetry records at: {output_file}")